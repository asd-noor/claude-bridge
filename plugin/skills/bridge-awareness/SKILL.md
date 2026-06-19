---
name: bridge-awareness
description: >
  Use bridge tools to coordinate with Claude Code sessions in other
  projects when making cross-repo changes, API updates, or breaking
  changes that affect dependencies.
---

# Bridge Awareness

Other Claude Code sessions on this machine may be connected to the same
bridge daemon. Each peer's `project_name` / `project_path` is a label
telling you *where they are running* — same project or different, it
doesn't matter. Use it to write messages that make sense in their context.

## When to use the bridge automatically (no user prompt needed)

- You are about to make a breaking change to an API, type, or interface
- You are upgrading a shared library or dependency
- You detect an import from a sibling repo in the workspace
- You are asked to "coordinate with" or "notify" another session

## Workflow

1. Call `list_peers` to see who else is connected and what they're
   working on.

2. Before a breaking change: `broadcast` what you're about to do and
   why (broadcast is rate-limited — don't loop on it).

3. To ask a specific peer a question: `send_message` with
   `expects_reply: true`. Replies arrive as push notifications you'll
   see in your next turn. If the peer is idle and won't reply soon,
   continue with your best guess.

4. When you receive a message:
   - `expects_reply: true` — reply via `send_message` with
     `in_reply_to` set to the question's `id`.
   - `in_reply_to` set — this is the answer to one of your questions.
   - Otherwise — fire-and-forget; reply if useful.

5. `poll_messages` is a manual catch-up — use it if you suspect you
   missed something (e.g. just after a daemon restart).

## Message content guidelines

- Be specific: include the symbol name, file path, and nature of change
- Include the new signature or type when relevant
- Ask concrete questions: "Does your code call getUser()?
  It's now getCurrentUser() and returns UserProfile."
