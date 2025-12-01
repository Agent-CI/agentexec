"""Worker process for running LangChain agents with agentexec."""

from uuid import UUID

from langchain.agents import AgentExecutor, create_react_agent
from langchain_core.prompts import PromptTemplate
from langchain_openai import ChatOpenAI
from pydantic import BaseModel
import agentexec as ax

from context import ResearchCompanyContext
from db import engine
from tools import analyze_financial_data, search_company_info


class ResearchCompanyResult(BaseModel):
    """Result from company research task."""

    summary: str
    financial_performance: str | None = None
    recent_news: str | None = None
    products_services: str | None = None
    team_structure: str | None = None


pool = ax.WorkerPool(engine=engine)


@pool.task("research_company")
async def research_company(
    agent_id: UUID,
    context: ResearchCompanyContext,
) -> ResearchCompanyResult:
    """Research a company using a LangChain ReAct agent with tools.

    This demonstrates:
    - Using LangChain with ReAct agent pattern
    - Automatic activity tracking via LangChainRunner
    - Agent self-reporting progress via report_activity tool
    - Type-safe context object (automatically deserialized from queue)
    - Typed result return value
    """
    # Type-safe context access with IDE autocomplete!
    company_name = context.company_name
    input_prompt = context.input_prompt or f"Research the company {company_name}."

    # Initialize the LLM
    llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)

    # Define the tools available to the agent
    tools = [
        search_company_info,
        analyze_financial_data,
    ]

    # Create a ReAct prompt template
    template = """You are a thorough company research analyst.
Research {company_name} and provide a comprehensive report covering:
- Financial performance and metrics
- Recent news and developments
- Products and services offered
- Team and organizational structure

Use the available tools to gather information and synthesize a detailed report.

IMPORTANT: Report your progress regularly using the report_activity tool.
Call report_activity(message, percentage) to update on your current task.
Always report your current activity before starting a new step.
Include a brief message about the task and percentage completion (0-100).

You have access to the following tools:

{tools}

Use the following format:

Question: the input question you must answer
Thought: you should always think about what to do
Action: the action to take, should be one of [{tool_names}]
Action Input: the input to the action
Observation: the result of the action
... (this Thought/Action/Action Input/Observation can repeat N times)
Thought: I now know the final answer
Final Answer: the final answer to the original input question

Begin!

Question: {input}
Thought:{agent_scratchpad}"""

    prompt = PromptTemplate.from_template(template)
    prompt = prompt.partial(company_name=company_name)

    # Create the ReAct agent
    agent = create_react_agent(llm, tools, prompt)
    agent_executor = AgentExecutor(
        agent=agent,
        tools=tools,
        verbose=True,
        handle_parsing_errors=True,
    )

    # Wrap with agentexec runner for activity tracking
    runner = ax.LangChainRunner(
        agent_id=agent_id,
        agent_executor=agent_executor,
        max_turns_recovery=True,
        wrap_up_prompt="Please summarize your findings and provide a final report.",
    )

    # Execute the agent
    result = await runner.run(
        input=input_prompt,
        max_iterations=15,
    )

    # Extract the output from LangChain's result dict
    output = result.get("output", "No output generated")

    print(f"✓ Completed research for {company_name} (agent_id: {agent_id})")
    print(f"Report preview: {output[:200]}...")

    # Return typed result
    return ResearchCompanyResult(
        summary=output,
        financial_performance="See report",
        recent_news="See report",
        products_services="See report",
        team_structure="See report",
    )


if __name__ == "__main__":
    print("Starting LangChain agent worker pool...")
    print(f"Workers: {ax.CONF.num_workers}")
    print(f"Queue: {ax.CONF.queue_name}")
    print("Press Ctrl+C to shutdown gracefully")

    # run() blocks and handles log streaming from workers
    pool.run()
