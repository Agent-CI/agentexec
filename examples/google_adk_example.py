"""Example usage of GoogleADKRunner with the Google Agent Development Kit.

This example demonstrates how to:
1. Create a Google ADK agent with tools
2. Use GoogleADKRunner for execution
3. Track activity with the built-in report_status tool
4. Control execution limits using RunConfig

Requirements:
    pip install google-adk agentexec

Note: You'll need to set up Google Cloud credentials for this to work.
Set the GOOGLE_API_KEY environment variable or configure ADC.
"""

import asyncio
import uuid

from google.adk.agents import LlmAgent
from google.adk.core.run_config import RunConfig

import agentexec as ax


# Example tool for the agent
def search_company_info(company_name: str) -> str:
    """Search for basic information about a company.

    Args:
        company_name: Name of the company to search for.

    Returns:
        Basic company information.
    """
    # This is a mock implementation
    return f"Company: {company_name}\nIndustry: Technology\nFounded: 2020\nEmployees: 500"


async def main():
    """Run a simple Google ADK agent with activity tracking."""

    # Generate a unique agent ID for tracking
    agent_id = uuid.uuid4()

    # Create the runner with activity tracking
    runner = ax.GoogleADKRunner(
        agent_id=agent_id,
        app_name="company_research",
        report_status_prompt="Use report_activity(message, percentage) to report your progress.",
    )

    # Create a Google ADK agent
    # Note: Include the runner's prompts and tools for activity tracking
    research_agent = LlmAgent(
        name="Company Research Agent",
        model="gemini-2.0-flash",
        instruction=f"""You are a thorough company research analyst.

When researching a company:
1. Use the search_company_info tool to gather information
2. Analyze the data you find
3. Provide a concise summary

{runner.prompts.report_status}""",
        tools=[
            search_company_info,
            runner.tools.report_status,  # Add the activity tracking tool
        ],
    )

    # Run the agent with execution control
    # RunConfig controls execution limits (default max_llm_calls=500)
    print(f"Starting research with agent_id: {agent_id}")
    print("-" * 60)

    run_config = RunConfig(
        max_llm_calls=100,  # Limit to 100 LLM calls to prevent runaway execution
    )

    result = await runner.run(
        agent=research_agent,
        input="Research Acme Corporation and provide a brief overview.",
        run_config=run_config,
    )

    # Extract and display the result
    print("\n" + "=" * 60)
    print("FINAL RESULT:")
    print("=" * 60)
    if result.final_output:
        print(result.final_output)
    else:
        print("No final output received")

    print(f"\nTotal events: {len(result.events)}")


async def streaming_example():
    """Example of using the streaming mode with event forwarding."""

    agent_id = uuid.uuid4()

    runner = ax.GoogleADKRunner(
        agent_id=agent_id,
        app_name="streaming_demo",
    )

    agent = LlmAgent(
        name="Streaming Agent",
        model="gemini-2.0-flash",
        instruction="You are a helpful assistant. Be concise.",
        tools=[runner.tools.report_status],
    )

    # Define an event forwarder to process events in real-time
    async def handle_event(event):
        """Process each event as it arrives."""
        print(f"[EVENT] {type(event).__name__}")
        if hasattr(event, "is_final_response") and event.is_final_response():
            print(f"[FINAL] {event.content.parts[0].text if event.content.parts else 'No text'}")

    print(f"Starting streaming agent with agent_id: {agent_id}")
    print("-" * 60)

    result = await runner.run_streamed(
        agent=agent,
        input="What is 2 + 2?",
        forwarder=handle_event,
    )

    print(f"\nStreaming complete. Total events: {len(result.events)}")


if __name__ == "__main__":
    # Run the basic example
    asyncio.run(main())

    # Uncomment to run the streaming example
    # asyncio.run(streaming_example())
