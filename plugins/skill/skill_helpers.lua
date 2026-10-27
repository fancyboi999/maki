local LINE_FMT = "%4d | %s"
local LINE_PAT = "^%s*%d+ | (.*)$"
local MENTION_SIGIL = "$"
-- Only these names can be mentioned. They need no escaping in a regex, and
-- ending on a word character is what lets `\b` close the mention.
local MENTIONABLE = "^[%w_][%w_-]*$"
local MENTIONABLE_END = "[%w_]$"

local M = {}

M.MENTION_SIGIL = MENTION_SIGIL

function M.parse_frontmatter(content)
  local rest = content:match("^%s*%-%-%-\n(.*)")
  if not rest then
    return {}, content
  end
  local end_pos = rest:find("\n%-%-%-")
  if not end_pos then
    return {}, content
  end
  local yaml_str = rest:sub(1, end_pos)
  local body = rest:sub(end_pos + 4):match("^%s*(.-)%s*$")
  local fm, _ = maki.yaml.decode(yaml_str)
  if not fm then
    fm = {}
  end
  return fm, body
end

function M.build_skill_list(skills)
  local sorted = {}
  for _, s in pairs(skills) do
    sorted[#sorted + 1] = s
  end
  table.sort(sorted, function(a, b)
    return a.name < b.name
  end)

  if #sorted == 0 then
    return "\n\n<available_skills>\nNo skills available.\n</available_skills>"
  end

  local lines = {}
  for _, s in ipairs(sorted) do
    lines[#lines + 1] = "- " .. s.name .. ": " .. s.description
  end
  return "\n\n<available_skills>\n" .. table.concat(lines, "\n") .. "\n</available_skills>"
end

-- `parse_output` must stay the exact inverse: the view is drawn from
-- this text, live and on restore.
function M.format_output(location, content)
  local lines = { location }
  for i, line in ipairs(maki.split(content, "\n")) do
    lines[i + 1] = string.format(LINE_FMT, i, line)
  end
  return table.concat(lines, "\n")
end

function M.parse_output(output)
  local location, rest = output:match("^([^\n]*)\n(.*)$")
  if not location then
    return nil
  end
  local lines = {}
  for i, line in ipairs(maki.split(rest, "\n")) do
    lines[i] = line:match(LINE_PAT)
    if not lines[i] then
      return nil
    end
  end
  return location, table.concat(lines, "\n")
end

local function mentionable_names(skills)
  local names = {}
  for name in pairs(skills) do
    if type(name) == "string" and name:match(MENTIONABLE) and name:match(MENTIONABLE_END) then
      names[#names + 1] = name
    end
  end
  table.sort(names)
  return names
end

-- `\B` keeps `cost$go` plain. Longer names go first because the regex takes
-- the first alternative that matches, and `$go-review` must not stop at `go`.
-- Without lookahead `-` still ends a word, so `$go-x` marks `$go` when no
-- skill is named `go-x`.
function M.mention_pattern(skills)
  local names = mentionable_names(skills)
  if #names == 0 then
    return nil
  end
  table.sort(names, function(a, b)
    if #a ~= #b then
      return #a > #b
    end
    return a < b
  end)
  return [[\B\$(?:]] .. table.concat(names, "|") .. [[)\b]]
end

-- A model may pass the name the way the user mentioned it.
function M.skill_name(name)
  if name:sub(1, #MENTION_SIGIL) == MENTION_SIGIL then
    return name:sub(#MENTION_SIGIL + 1)
  end
  return name
end

function M.mention_items(skills, query)
  local items = {}
  for _, found in ipairs(maki.text.fuzzy_list(query, mentionable_names(skills))) do
    items[#items + 1] = {
      text = MENTION_SIGIL .. found.text,
      label = skills[found.text].description,
      score = found.score,
    }
  end
  return items
end

return M
