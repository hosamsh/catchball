# catchball
Put task files in `/tasks`and pass them around.


```bash
./run.sh --worker claude --reviewer codex
./run.sh --worker codex --reviewer claude --retries 3
./run.sh --worker claude --reviewer codex --tasks ./my-tasks
```
