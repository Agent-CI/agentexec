from uuid import UUID

from pydantic import BaseModel
from agents import Agent

import agentexec as ax

from context import ResearchCompanyContext
from db import engine
from tools import analyze_financial_data, search_company_info


ax.Base.metadata.create_all(engine)


class ResearchCompanyResult(BaseModel):
    financial_performance: str
    recent_news: str
    products_services: str
    team_structure: str


pool = ax.Pool(engine=engine)


@pool.task("research_company")
async def research_company(
    agent_id: UUID,
    context: ResearchCompanyContext,
) -> ResearchCompanyResult:
    """Research a company using an AI agent with tools.

    This demonstrates:
    - Using OpenAI Agents SDK with function tools
    - Automatic activity tracking via OpenAIRunner
    - Agent self-reporting progress via update_status tool
    - Type-safe context object (automatically deserialized from queue)
    """
    # Type-safe context access with IDE autocomplete!
    company_name = context.company_name
    input_prompt = context.input_prompt or f"Research the company {company_name}."

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
        output_type=ResearchCompanyResult,
    )

    result = await runner.run(
        agent=research_agent,
        input=input_prompt,
        max_turns=15,
    )
    # `result` is a native OpenAI Agents `RunResult` object
    return result.final_output_as(ResearchCompanyResult)


if __name__ == "__main__":
    print("Starting agent-runner worker pool...")
    print(f"Workers: {ax.CONF.num_workers}")
    print(f"Queue: {ax.CONF.queue_name}")
    print("Press Ctrl+C to shutdown gracefully")

    # run() blocks and handles log streaming from workers
    pool.run()
