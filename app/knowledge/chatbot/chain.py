import json
from langchain.memory import ConversationBufferWindowMemory
from langchain.chat_models import ChatOpenAI
from langchain.tools import tool
from langchain.agents import Tool, AgentExecutor, LLMSingleActionAgent, AgentOutputParser
from langchain.prompts import StringPromptTemplate
from langchain import LLMChain
from typing import List, Union
from langchain.schema import AgentAction, AgentFinish, OutputParserException
import re
from .generation import Generation
import os

def is_json(myjson):
  try:
    json.loads(myjson)
  except ValueError as e:
    return False
  return True


def _build_tools(generator: Generation, memory):
    @tool("Data Collection", return_direct=False)
    def data_collection(query: str):
        """useful for when you need first to answer questions about getting data from database, or you first need to collect the data.
      Like:
        - Top 10 products.
      The input to this tool should be a user question. Note that you can not add or update or delete data point, only retrieve data from database."""
        df_markdown, df, sql = generator.data_collection(query)

        return f"""
            Collected sql: {sql}
            Collected Data: {df.head(20).to_markdown()}
            """


    @tool("Data Collection keyword", return_direct=True)
    def data_collection_keyword(query: str):
        """useful for when you need to answer question that contains keywords 'Get,List,Retrieve' about getting data from database and return directly.
        Like:
            - Get: Top 10 products.
            - List: Top 10 products.
            - Retrieve: Top 10 products.
        The input to this tool should be a user question. Note that you can not add or update or delete data point, only retrieve data from database."""
        df_markdown, df, sql = generator.data_collection(query)
        memory.save_context({
            "input": query
        }, {"output": f"""
            Collected sql: {sql}
            Collected Data: {df.head(20).to_markdown()}
            """})
        return df.head(20).to_markdown()


    @tool("Data Visualization", return_direct=True)
    def data_visualization(query: str):
        """useful for when you need to visualization data.
        The input to this tool should be a forward slash separated list of question and sql.
        For example: `Visualize a bar chart/SELECT * FROM PRODUCT LIMIT 10` would be the input if you wanted to user question is Visualize a bar chart and the query is SELECT * FROM PRODUCT LIMIT 10."""
        question, sql = query.split('/')
        return generator.data_visualization_df_sql(question, sql.replace('"', '').replace('`', ''), 20)


    @tool("Data Visualization keyword", return_direct=True)
    def data_visualization_keyword(query: str):
        """useful for when you need to visualization data contains keywords 'Draw,Plot,Visualize' and return directly.
        Like:
            - Draw: Top 10 products.
            - Plot: Top 10 products.
            - Visualize: Top 10 products.
        The input to this tool should be a forward slash separated list of question and sql.
        For example: `Visualize a bar chart/SELECT * FROM PRODUCT LIMIT 10` would be the input if you wanted to user question is Visualize a bar chart and the query is SELECT * FROM PRODUCT LIMIT 10."""
        question, sql = query.split('/')
        return generator.data_visualization_df_sql(question, sql.replace('"', '').replace('`', ''), 20)


    @tool('Data Analysis 1', return_direct=True)
    def data_analysis1(query: str):
        """useful for when you need to analyzing data.
        The input to this tool should be a semicolon separated list of question and data.
        For example: `Why is product 1 potential;|    |     a |    b |
    |---:|------:|-----:|
    |  0 |     3 | 2222 |
    |  1 | 10000 |    3 |` would be the input if you wanted to user question is why is product 1 potential and the data is |    |     a |    b |
    |---:|------:|-----:|
    |  0 |     3 | 2222 |
    |  1 | 10000 |    3 |"""
        question, df_markdown = query.split(';')
        return generator.data_insights_generation_1(question, df_markdown)


    @tool('Data Analysis keyword', return_direct=True)
    def data_analysis1_keyword(query: str):
        """useful for when you need to analyzing data that contains keyword 'Analysis Data' and return directly.
        Like:
            - Analysis Data: Top 10 product.
        The input to this tool should be a semicolon separated list of question and data.
        For example: `Give me some insights about the data;|    |     a |    b |
    |---:|------:|-----:|
    |  0 |     3 | 2222 |
    |  1 | 10000 |    3 |` would be the input if you wanted to user question is Give me some insights and the data is |    |     a |    b |
    |---:|------:|-----:|
    |  0 |     3 | 2222 |
    |  1 | 10000 |    3 |"""
        question, df_markdown = query.split(';')
        return generator.data_insights_generation_1(question, df_markdown)

    @tool('Data Analysis 2', return_direct=True)
    def data_analysis2(query: str):
        """useful for when you need to analyzing chart.
        The input to this tool should be a semicolon separated list of question and vega-lite json.
        For example: `Give me some insights about the chart;{"mark": "bar", "encoding": {"x": {"field": "InvoiceDate", "type": "temporal"}, "y": {"aggregate": "count", "type": "quantitative"}}, "data": {"values": [{"InvoiceDate": "2009-12-0800:00:00"}, {"InvoiceDate": "2010-01-1800:00:00"}]}}`
        would be the input if you wanted to user question is Give me some insights and the vege-lite is {"mark": "bar", "encoding": {"x": {"field": "InvoiceDate", "type": "temporal"}, "y": {"aggregate": "count", "type": "quantitative"}}, "data": {"values": [{"InvoiceDate": "2009-12-0800:00:00"}, {"InvoiceDate": "2010-01-1800:00:00"}]}}
        """
        question, vega_json = query.split(';')
        return generator.data_analysis_2_vega_json(question, vega_json)


    @tool('Chart Analysis keyword', return_direct=True)
    def data_analysis2_keyword(query: str):
        """useful for when you need to analyzing chart that contains keyword 'Analysis Chart' and return directly.
        Like:
         - Analysis Chart: Top 10 products.
        The input to this tool should be a semicolon separated list of question and vega-lite json.
        For example: `Give me some insights about the chart;{"mark": "bar", "encoding": {"x": {"field": "InvoiceDate", "type": "temporal"}, "y": {"aggregate": "count", "type": "quantitative"}}, "data": {"values": [{"InvoiceDate": "2009-12-0800:00:00"}, {"InvoiceDate": "2010-01-1800:00:00"}]}}`
        would be the input if you wanted to user question is Give me some insights and the vege-lite is {"mark": "bar", "encoding": {"x": {"field": "InvoiceDate", "type": "temporal"}, "y": {"aggregate": "count", "type": "quantitative"}}, "data": {"values": [{"InvoiceDate": "2009-12-0800:00:00"}, {"InvoiceDate": "2010-01-1800:00:00"}]}}
        """
        question, vega_json = query.split(';')
        return generator.data_analysis_2_vega_json(question, vega_json)


    @tool('Data Analysis 3', return_direct=True)
    def data_analysis3(query: str):
        """useful for when you need to analyzing both data and chart.
        The input to this tool should be a semicolon separated list of question, data and vega-lite json.
        For example: `Give me some insights about data and chart;|    |     a |    b |
    |---:|------:|-----:|
    |  0 |     3 | 2222 |
    |  1 | 10000 |    3 |;{"mark": "bar", "encoding": {"x": {"field": "InvoiceDate", "type": "temporal"}, "y": {"aggregate": "count", "type": "quantitative"}}, "data": {"values": [{"InvoiceDate": "2009-12-0800:00:00"}, {"InvoiceDate": "2010-01-1800:00:00"}]}}`
        would be the input if you wanted to user question is Give me some insights about data and chart, the data is |    |     a |    b |
    |---:|------:|-----:|
    |  0 |     3 | 2222 |
    |  1 | 10000 |    3 | and the vega-lite is {"mark": "bar", "encoding": {"x": {"field": "InvoiceDate", "type": "temporal"}, "y": {"aggregate": "count", "type": "quantitative"}}, "data": {"values": [{"InvoiceDate": "2009-12-0800:00:00"}, {"InvoiceDate": "2010-01-1800:00:00"}]}}
        """
        question, df_markdown, vega_json = query.split(';')
        return generator.data_analysis_3(question, df_markdown, vega_json)

    tools = [
        data_collection,
        data_collection_keyword,
        data_visualization,
        # data_visualization_keyword,
        data_analysis1,
        data_analysis1_keyword,
        data_analysis2,
        data_analysis2_keyword
        # data_analysis3
    ]
    return tools

def build_prompt_sql_generation(org_name):
    DATABASE_SCHEMAS = """Table {customer}, columns = [*,customer_id,customer_first_name,customer_last_name,customer_email,customer_date_of_birth,customer_phone_number,customer_gender,customer_job_title,customer_location,customer_account_date]
    Table {product}, columns = [*,product_id,product_name,product_url,product_description,product_category_1,product_category_2,product_category_3,product_quantity,product_price,product_from_date,product_to_date]
    Table {transaction}, columns = [*,transaction_id,transaction_customer_id,transaction_peusdo_user,transaction_revenue_value,transaction_tax_value, transaction_refund_value, transaction_shipping_value,transaction_shipping_type,transaction_shipping_address,transaction_status,transaction_time]
    Table {transaction_product}, columns = [*,transaction_id,product_id,transaction_product_quantity,transaction_product_description,transaction_product_extra_value_1,transaction_product_extra_value_2,transaction_product_extra_value_3]
    Table {event}, columns = [*,event_id,event_type,event_customer_id,event_touchpoint_type,event_peusdo_user,event_device_category,event_device_brand,event_device_os,event_device_browser,event_device_language,event_device_geography_continent,event_device_geography_sub_continent,event_geography_country,event_geography_city,event_session_id,event_page_title,event_page_url,event_traffic_source,event_ip_address,event_keyword,event_start_time,event_end_time,event_is_like,event_rate,event_review]
    Table {event_product}, columns = [*,event_id,product_id,event_product_description,event_product_extra_value_1,event_product_extra_value_2,event_product_extra_value_3]
    Foreign_keys = [{customer}.customer_id = {transaction}.transaction_customer_id,{customer}.customer_id = {event}.event_customer_id,{product}.product_id = {transaction_product}.product_id,{product}.product_id = {transaction_product}.product_id,{product}.product_id = {event_product}.product_id,{transaction}.transaction_id = {transaction_product}.transaction_id,{event}.event_id = {event_product}.event_id]"""
    f = dict(customer=f'view_{org_name}_data_customer',
             product=f'view_{org_name}_data_product',
             transaction=f'view_{org_name}_data_transaction',
             transaction_product=f'view_{org_name}_data_transaction_product',
             event=f'view_{org_name}_data_event',
             event_product=f'view_{org_name}_data_event_product')
    return DATABASE_SCHEMAS.format(**f)

template_with_history = """Answer the following questions as best you can, but speaking as a data analysis expert. You have access to the following tools:

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

Begin! Remember to speak as a data analysis expert when giving your final answer.

Previous conversation history:
{history}

New question: {input}
{agent_scratchpad}"""


class CustomPromptTemplate(StringPromptTemplate):
    # The template to use
    template: str
    # The list of tools available
    tools: List[Tool]

    def format(self, **kwargs) -> str:
        # Get the intermediate steps (AgentAction, Observation tuples)
        # Format them in a particular way
        intermediate_steps = kwargs.pop("intermediate_steps")
        thoughts = ""
        for action, observation in intermediate_steps:
            thoughts += action.log
            thoughts += f"\nObservation: {observation}\nThought: "
        # Set the agent_scratchpad variable to that value
        kwargs["agent_scratchpad"] = thoughts
        # Create a tools variable from the list of tools provided
        kwargs["tools"] = "\n".join([f"{tool.name}: {tool.description}" for tool in self.tools])
        # Create a list of tool names for the tools provided
        kwargs["tool_names"] = ", ".join([tool.name for tool in self.tools])
        return self.template.format(**kwargs)


class CustomOutputParser(AgentOutputParser):

    def parse(self, llm_output: str) -> Union[AgentAction, AgentFinish]:
        # Check if agent should finish
        print("LLM Output: ", llm_output)
        if "Final Answer:" in llm_output:
            return AgentFinish(
                # Return values is generally always a dictionary with a single `output` key
                # It is not recommended to try anything else at the moment :)
                return_values={"output": llm_output.split("Final Answer:")[-1].strip()},
                log=llm_output,
            )
        # Parse out the action and action input
        regex = r"Action\s*\d*\s*:(.*?)\nAction\s*\d*\s*Input\s*\d*\s*:[\s]*(.*)"
        match = re.search(regex, llm_output, re.DOTALL)
        if not match:
            raise OutputParserException(f"Could not parse LLM output: `{llm_output}`")
        action = match.group(1).strip()
        action_input = match.group(2)
        # Return the action and action input
        return AgentAction(tool=action, tool_input=action_input.strip(" ").strip('"'), log=llm_output)



class Chain:

    def __init__(self, org_name, histories):
        self.database_schemas = build_prompt_sql_generation(org_name)
        self.GPT_API_KEY = os.environ.get("OPEN_AI_API_KEY", "")
        self.memory = self.build_memory(histories)

    def build_memory(self, histories):
        memory = ConversationBufferWindowMemory(k=2)
        for history in histories:
            '''
            {
            "input": "",
            "output": ""
            }
            '''
            memory.save_context({"input": history['input']}, {"output": history['output']})
        return memory
    def build_tools(self, memory):
        generator = Generation(self.database_schemas)
        tools = _build_tools(generator, memory=memory)
        return tools
    def build_agent_chain(self):
        tools = self.build_tools(self.memory)

        tool_names = [tool.name for tool in tools]
        prompt_with_history = CustomPromptTemplate(
            template=template_with_history,
            tools=tools,
            # This omits the `agent_scratchpad`, `tools`, and `tool_names` variables because those are generated dynamically
            # This includes the `intermediate_steps` variable because that is needed
            input_variables=["input", "intermediate_steps", "history"]
        )

        llm = ChatOpenAI(temperature=0, openai_api_key=self.GPT_API_KEY,
                         model_name="gpt-4")

        llm_chain = LLMChain(llm=llm, prompt=prompt_with_history)
        agent = LLMSingleActionAgent(
            llm_chain=llm_chain,
            output_parser=CustomOutputParser(),
            stop=["\nObservation:"],
            allowed_tools=tool_names,
        )
        agent_chain = AgentExecutor.from_agent_and_tools(agent=agent, tools=tools, verbose=True, memory=self.memory,
                                                         handle_parsing_errors=True
        )

        return agent_chain


    def run(self, prompt_input):
        agent_chain = self.build_agent_chain()
        data = agent_chain.run(input=prompt_input)

        messages = self.memory.chat_memory.messages
        histories = []
        for i in range(len(messages)):
            if i % 2 == 0 and i > 0:
                _input = messages[i].content
                output = messages[i + 1].content
                histories.append({
                    'input': _input,
                    'output': output
                })

        if not is_json(data):
            return {
                "type": "text",
                "content": data,
                'histories': histories
            }
        else:
            return {
                "type": 'vega',
                "content": data,
                "histories": histories
            }
