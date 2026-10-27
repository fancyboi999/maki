//! The bundled `task_stop` tool against calls moved to the background. A
//! parker tool registers a move hook and waits, standing in for a long
//! command.

use std::collections::HashMap;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use maki_agent::agent::tool_dispatch;
use maki_agent::tools::test_support::stub_seed;
use maki_agent::tools::{CallOrigin, ToolAudience, ToolContext, ToolRegistry};
use maki_agent::{
    AgentEvent, AgentSeed, CallScope, CancelToken, CancelTrigger, EventSender, SessionMailbox,
    ToolDoneEvent,
};
use maki_config::PluginsConfig;
use maki_lua::{MoveOutcome, PluginHost, RestoreItem, RestoreReason, UiAction};
use maki_storage::id::MakiId;
use serde_json::{Value, json};
use test_case::test_case;

const STOP_TOOL: &str = "task_stop";
const PARK_TOOL: &str = "park";
const PARKER_PLUGIN: &str = "task_stop_parker";
const REDACTOR_PLUGIN: &str = "redactor";
const REDACTED: &str = "[redacted]";
const RECEIPT: &str = "receipt: moved to the background";
const PARTIAL_OUTPUT: &str = "partial output so far";
const STOPPED_REASON: &str = "stopped";
const CANCELLED_REASON: &str = "cancelled";
const STOP_REQUESTED_FRAGMENT: &str = "Stop requested";
const NO_RUNNING_TASK_FRAGMENT: &str = "no running background task";
const FIRST_ID: &str = "toolu_first";
const SECOND_ID: &str = "toolu_second";
const MISSING_ID: &str = "toolu_missing";
const HEADER_ID: &str = "toolu_header";
const SUBAGENT_TASK_ID: &str = "toolu_subagent";
const RUN_ID: u64 = 1;
const DEADLINE: Duration = Duration::from_secs(60);

/// `honor_cancel` returns at once and finishes with the partial output and
/// the cancel reason when the call is cancelled, as bash does with its job. Without it the handler
/// stays parked and ignores the stop, so only the runtime's abandon timeout
/// ends it.
const PARKER_SRC: &str = r#"
maki.api.register_tool({
  name = "park",
  description = "waits until cancelled",
  schema = {
    type = "object",
    properties = { honor_cancel = { type = "boolean" } },
  },
  audiences = { "main" },
  handler = function(input, ctx)
    ctx:on_background(function()
      return "receipt: moved to the background"
    end)
    if not input.honor_cancel then
      maki.async.await(1, function() end)
    end
    maki.async.on_cancel(function(reason)
      ctx:finish({ llm_output = "partial output so far: " .. reason })
    end)
    maki.fn.jobstart("sleep 600", {})
    return nil
  end,
})
"#;

/// An output layer on `park` that hides whatever the call ended with. It
/// leaves the receipt alone, so the move still reads as one.
const REDACTOR_SRC: &str = r#"
maki.api.set_slot("tool.park.output", function(prev, out, ctx)
  if not ctx.receipt then
    out.text = "[redacted]"
  end
  return prev(out, ctx)
end)
"#;

struct Chat {
    mailbox: SessionMailbox,
    seed: Arc<AgentSeed>,
    _run_trigger: CancelTrigger,
    run: CancelToken,
}

fn chat() -> Chat {
    let id = MakiId::generate();
    let mailbox = SessionMailbox::register(id);
    let seed = mailbox.publish_seed(stub_seed(id));
    let (run_trigger, run) = CancelToken::new();
    Chat {
        mailbox,
        seed,
        _run_trigger: run_trigger,
        run,
    }
}

fn host() -> (Arc<ToolRegistry>, PluginHost) {
    let reg = Arc::new(ToolRegistry::new());
    let mut host = PluginHost::new(Arc::clone(&reg)).unwrap();
    host.load_builtins(&PluginsConfig::from_plugins(HashMap::new()))
        .unwrap();
    host.load_source(PARKER_PLUGIN, PARKER_SRC).unwrap();
    (reg, host)
}

/// Through dispatch, the one path that lands a moved call's final reply.
fn exec(reg: &Arc<ToolRegistry>, name: &str, input: Value, ctx: &ToolContext) -> ToolDoneEvent {
    let ctx = ToolContext {
        registry: Arc::clone(reg),
        ..ctx.clone()
    };
    let id = ctx.tool_use_id.clone().unwrap_or_default();
    smol::block_on(tool_dispatch::run(
        id,
        name,
        &input,
        &ctx,
        CallOrigin::Model,
    ))
}

fn text(done: ToolDoneEvent) -> Result<String, String> {
    let text = done.output.as_text();
    if done.is_error { Err(text) } else { Ok(text) }
}

/// A ctx of the chat's own agent, whose run reads the chat's mailbox.
fn agent_ctx(chat: &Chat) -> ToolContext {
    let mut ctx = chat.seed.tool_context(CancelToken::none());
    ctx.mailbox = Some(chat.mailbox.clone());
    ctx
}

fn stop(reg: &Arc<ToolRegistry>, chat: &Chat, id: &str) -> Result<String, String> {
    stop_as(reg, &agent_ctx(chat), id)
}

fn stop_as(reg: &Arc<ToolRegistry>, ctx: &ToolContext, id: &str) -> Result<String, String> {
    text(exec(reg, STOP_TOOL, json!({ "id": id }), ctx))
}

/// Runs a `park` call as the chat's model call `id` and moves it. Returns once
/// the receipt answered the call.
fn park_and_move(
    reg: &Arc<ToolRegistry>,
    host: &PluginHost,
    chat: &Chat,
    id: &str,
    honor_cancel: bool,
) {
    let (events_tx, events_rx) = flume::unbounded();
    let scope = Arc::new(CallScope::new(id.to_owned(), &chat.run));
    let mut ctx = agent_ctx(chat);
    ctx.event_tx = EventSender::new(events_tx, RUN_ID);
    ctx.tool_use_id = Some(id.to_owned());
    ctx.cancel = scope.cancel().clone();
    ctx.call = Some(scope);

    let (receipt_tx, receipt_rx) = flume::bounded(1);
    let reg = Arc::clone(reg);
    thread::spawn(move || {
        let input = json!({ "honor_cancel": honor_cancel });
        receipt_tx.send(exec(&reg, PARK_TOOL, input, &ctx)).unwrap();
    });

    let deadline = Instant::now() + DEADLINE;
    loop {
        let envelope = events_rx
            .recv_deadline(deadline)
            .expect("the call never announced it can move");
        if matches!(
            envelope.event,
            AgentEvent::ToolMovable { movable: true, .. }
        ) {
            break;
        }
    }
    let outcome = host
        .event_handle()
        .request_background(id.to_owned())
        .recv_timeout(DEADLINE)
        .unwrap();
    assert_eq!(outcome, MoveOutcome::Moved);

    let receipt = receipt_rx.recv_timeout(DEADLINE).unwrap();
    assert!(receipt.backgrounded);
    assert!(text(receipt).unwrap().contains(RECEIPT));
}

/// The chat's notice for a task that ended on its own. The host sends it
/// before the task tab's final reply, so that reply is the signal to look.
fn next_notice(chat: &Chat, ui: &flume::Receiver<UiAction>) -> (String, bool) {
    loop {
        let action = ui.recv_timeout(DEADLINE).expect("no final reply arrived");
        if let UiAction::PluginTaskEvent { envelope, .. } = action
            && matches!(envelope.event, AgentEvent::ToolDone(_))
        {
            break;
        }
    }
    let mut woken = chat.mailbox.claim_wake();
    let woke = !woken.is_empty();
    if !woke {
        woken = chat.mailbox.drain();
    }
    let notice = woken.first().expect("the notice precedes the final reply");
    (notice.first_text_content().unwrap().to_owned(), woke)
}

#[test]
fn task_stop_is_registered_for_the_main_audience_only() {
    let (reg, _host) = host();
    let entry = reg.get(STOP_TOOL).expect("task_stop is always registered");
    assert_eq!(entry.tool.audience(), ToolAudience::MAIN);
}

#[test]
fn stopping_a_moved_call_returns_its_output_and_sends_no_notice() {
    let (reg, host) = host();
    let chat = chat();
    park_and_move(&reg, &host, &chat, FIRST_ID, true);

    let output = stop(&reg, &chat, FIRST_ID).unwrap();

    assert!(output.contains(PARTIAL_OUTPUT), "{output}");
    assert!(chat.mailbox.drain().is_empty());
    assert!(chat.mailbox.unsettled().is_empty());
}

/// The stop cancels the call before its final reply exists, and the layer
/// still has its say on that reply before the model reads it.
#[test]
fn a_stopped_calls_output_passes_its_output_layers() {
    let (reg, host) = host();
    host.load_source(REDACTOR_PLUGIN, REDACTOR_SRC).unwrap();
    let chat = chat();
    park_and_move(&reg, &host, &chat, FIRST_ID, true);

    let output = stop(&reg, &chat, FIRST_ID).unwrap();

    assert_eq!(output, REDACTED);
}

/// The cancel hook learns who stopped the call, so its marker can say so.
#[test_case(true, STOPPED_REASON ; "by_the_model")]
#[test_case(false, CANCELLED_REASON ; "by_the_user")]
fn the_cancel_hook_names_who_stopped_the_call(by_model: bool, reason: &str) {
    let (reg, host) = host();
    host.ui_attachment().attach();
    let ui = host.ui_action_rx();
    let chat = chat();
    park_and_move(&reg, &host, &chat, FIRST_ID, true);

    let output = if by_model {
        stop(&reg, &chat, FIRST_ID).unwrap()
    } else {
        host.event_handle().cancel_plugin_session(FIRST_ID);
        next_notice(&chat, &ui).0
    };

    assert!(
        output.contains(&format!("{PARTIAL_OUTPUT}: {reason}")),
        "{output}"
    );
}

#[test]
fn a_stop_that_times_out_still_delivers_the_late_result_quietly() {
    let (reg, host) = host();
    host.ui_attachment().attach();
    let ui = host.ui_action_rx();
    let chat = chat();
    park_and_move(&reg, &host, &chat, FIRST_ID, false);

    let output = stop(&reg, &chat, FIRST_ID).unwrap();
    assert!(output.contains(STOP_REQUESTED_FRAGMENT), "{output}");

    let (notice, woke) = next_notice(&chat, &ui);
    assert!(notice.contains(PARK_TOOL), "{notice}");
    assert!(notice.contains(FIRST_ID), "{notice}");
    assert!(!woke, "a stopped task must not wake the chat");
}

#[test]
fn an_unknown_id_is_an_error_listing_the_running_ids() {
    let (reg, host) = host();
    let chat = chat();
    park_and_move(&reg, &host, &chat, FIRST_ID, true);
    park_and_move(&reg, &host, &chat, SECOND_ID, true);

    let err = stop(&reg, &chat, MISSING_ID).unwrap_err();

    assert!(err.contains(NO_RUNNING_TASK_FRAGMENT), "{err}");
    assert!(err.contains(FIRST_ID), "{err}");
    assert!(err.contains(SECOND_ID), "{err}");
    assert!(
        stop(&reg, &chat, FIRST_ID)
            .unwrap()
            .contains(PARTIAL_OUTPUT)
    );
    assert!(
        stop(&reg, &chat, SECOND_ID)
            .unwrap()
            .contains(PARTIAL_OUTPUT)
    );
}

#[test]
fn another_chats_id_is_refused_and_its_task_keeps_running() {
    let (reg, host) = host();
    let owner = chat();
    let other = chat();
    park_and_move(&reg, &host, &owner, FIRST_ID, true);

    let err = stop(&reg, &other, FIRST_ID).unwrap_err();

    assert!(err.contains(NO_RUNNING_TASK_FRAGMENT), "{err}");
    assert!(other.mailbox.drain().is_empty());
    assert!(
        stop(&reg, &owner, FIRST_ID)
            .unwrap()
            .contains(PARTIAL_OUTPUT)
    );
}

/// The chat's model moved the call, so a subagent of the chat, with a run
/// of its own, has nothing to stop.
#[test]
fn a_subagent_cannot_stop_a_task_its_chat_moved() {
    let (reg, host) = host();
    let chat = chat();
    park_and_move(&reg, &host, &chat, FIRST_ID, true);
    let mut subagent_ctx = chat.seed.tool_context(CancelToken::none());
    subagent_ctx.task_id = Some(Arc::from(SUBAGENT_TASK_ID));
    subagent_ctx.mailbox = Some(SessionMailbox::register(MakiId::generate()));

    let err = stop_as(&reg, &subagent_ctx, FIRST_ID).unwrap_err();

    assert!(err.contains(NO_RUNNING_TASK_FRAGMENT), "{err}");
    assert!(
        stop(&reg, &chat, FIRST_ID)
            .unwrap()
            .contains(PARTIAL_OUTPUT)
    );
}

#[test]
fn restore_shows_the_id_as_the_header() {
    let (_reg, host) = host();
    let handle = host.event_handle();
    let (tx, rx) = flume::unbounded();
    let item = RestoreItem {
        tool: Arc::from(STOP_TOOL),
        tool_use_id: "restore_id".to_owned(),
        output: PARTIAL_OUTPUT.to_owned(),
        input: json!({ "id": HEADER_ID }),
        is_error: false,
        tool_output_lines: maki_config::ToolOutputLines::DEFAULT,
        theme_gen: None,
        clicks: Vec::new(),
        state: None,
        session_id: None,
        task_id: None,
        reason: RestoreReason::default(),
    };

    handle.request_restore(item, EventSender::new(tx, RUN_ID));
    handle.wait_restore_complete_for_test();

    let header = rx
        .drain()
        .find_map(|env| match env.event {
            AgentEvent::ToolHeaderSnapshot { snapshot, .. } => Some(snapshot.text()),
            _ => None,
        })
        .expect("restore emits a header");
    assert_eq!(header, HEADER_ID);
}
