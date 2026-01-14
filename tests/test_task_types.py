"""Type checking tests for TaskHandler protocols.

Run `ty check tests/test_task_types.py` to verify.
"""

from uuid import UUID

from pydantic import BaseModel

from agentexec.core.task import TaskHandler


def _mock_register(func: TaskHandler) -> None:
    pass


class MyContext(BaseModel):
    query: str


class MyResult(BaseModel):
    answer: str


# Sync function handler
def sync_handler(*, agent_id: UUID, context: MyContext) -> MyResult:
    return MyResult(answer="test")


_mock_register(sync_handler)


# Async function handler
async def async_handler(*, agent_id: UUID, context: MyContext) -> MyResult:
    return MyResult(answer="test")


_mock_register(async_handler)


# Sync classmethod handler
class SyncAgent:
    @classmethod
    def run(cls, *, agent_id: UUID, context: MyContext) -> MyResult:
        return MyResult(answer="test")


_mock_register(SyncAgent.run)


# Async classmethod handler
class AsyncAgent:
    @classmethod
    async def run(cls, *, agent_id: UUID, context: MyContext) -> MyResult:
        return MyResult(answer="test")


_mock_register(AsyncAgent.run)
