from uuid import UUID

from agents import Agent
from sqlalchemy.orm import Session
import agentexec as ax

from .main import engine
from .tools import analyze_financial_data, search_company_info


pool = ax.WorkerPool(engine=engine)


@pool.task("research_company")
async def research_company(agent_id: UUID, payload: dict):
    """Research a company using an AI agent with tools.

    This demonstrates:
    - Using OpenAI Agents SDK with function tools
    - Automatic activity tracking via OpenAIRunner
    - Agent self-reporting progress via update_status tool
    """
    # TODO stronger typing for payload
    company_name = payload.get("company_name", "Unknown Company")
    input_prompt = payload.get("input_prompt", f"Research the company {company_name}.")

    runner = ax.OpenAIRunner(
        agent_id,
        max_turns_recovery=True,
        wrap_up_prompt="Please summarize your findings and provide a final report.",
    )

    research_agent = Agent(
        name="Company Research Agent",
        instructions=f"""You are a thorough company research analyst.
        Research {company_name} and provide a comprehensive report covering:
        - Financial performance and metrics
        - Recent news and developments
        - Products and services offered
        - Team and organizational structure

        Use the available tools to gather information and synthesize a detailed report.

        {runner.prompts.report_status}""",
        tools=[
            search_company_info,
            analyze_financial_data,
            runner.tools.report_status,
        ],
        model="gpt-4o-mini",
    )

    result = await runner.run(
        agent=research_agent,
        input=input_prompt,
        max_turns=15,
    )
    # `result` is a native OpenAI Agents `RunResult` object
    report = result.final_output

    print(f"✓ Completed research for {company_name} (agent_id: {agent_id})")
    print(f"Report preview: {report[:200]}...")


if __name__ == "__main__":
    print("Starting agent-runner worker pool...")
    print(f"Workers: {ax.CONF.num_workers}")
    print(f"Queue: {ax.CONF.queue_name}")
    print("Press Ctrl+C to shutdown gracefully")

    try:
        pool.start()

    except KeyboardInterrupt:
        print("\nShutting down worker pool...")
        pool.shutdown()

        # Mark any pending activities as canceled
        with Session(engine) as session:
            canceled = ax.activity.cancel_pending(session)
            session.commit()
            print(f"Canceled {canceled} pending agents")

        print("Worker pool stopped.")
