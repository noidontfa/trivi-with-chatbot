from io import StringIO
from textwrap import dedent
import sqlite3
import pandas as pd
import openai
import copy
import json
from altair.utils.data import to_values
import altair as alt
from retry import retry
import re
import os

from data.modules.db import Database

GPT_API_KEY = os.environ.get("OPEN_AI_API_KEY", "")
openai.api_key = GPT_API_KEY


# alt.renderers.enable('altair_viewer', inline=True)


def _extract_tag_content(s: str, tag: str) -> str:
    m = re.search(rf"<{tag}>(.*)</{tag}>", s, re.MULTILINE | re.DOTALL)
    if m:
        return m.group(1)
    else:
        m = re.search(rf"<{tag}>(.*)<{tag}>", s, re.MULTILINE | re.DOTALL)
        if m:
            return m.group(1)
    return ""


class LLM:
    def __init__(self, use_llm='PALM'):
        self.use_llm = use_llm
    def GPT4_generation(self, prompt, system_prompt=None):
        if system_prompt:
            response = openai.chat.completions.create(
                model="gpt-4",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": prompt}],
                stream=False,
                temperature=0.0,
                top_p=1.0,
                frequency_penalty=0.0,
                presence_penalty=0.0,
            )
        else:
            response = openai.chat.completions.create(
                model="gpt-4",
                messages=[{"role": "user", "content": prompt}],
                stream=False,
                temperature=0.0,
                top_p=1.0,
                frequency_penalty=0.0,
                presence_penalty=0.0,
            )
        return response.choices[0].message.content

    def GPT35_generation(self, prompt, system_prompt=None):
        if system_prompt:
            response = openai.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": prompt}],
                stream=False,
                temperature=0.0,
                frequency_penalty=0.0,
                presence_penalty=0.0,
            )
        else:
            response = openai.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[{"role": "user", "content": prompt}],
                stream=False,
                temperature=0.0,
                frequency_penalty=0.0,
                presence_penalty=0.0,
            )
        return response.choices[0].message.content

    def generate(self, prompt, use_llm='PALM'):
        if use_llm == 'GPT3.5':
            return self.GPT35_generation(prompt)
        if use_llm == 'GPT4-v2':
            return self.GPT4_generation_version_2(prompt)
        return self.GPT4_generation(prompt)

    def generate2(self, prompt, system_prompt, use_llm='PALM'):
        if use_llm == 'GPT3.5':
            return self.GPT35_generation(prompt, system_prompt)
        if use_llm == 'GPT4-v2':
            return self.GPT4_generation_version_2(prompt, system_prompt)
        return self.GPT4_generation(prompt,system_prompt)

    def GPT4_generation_version_2(self, prompt, system_prompt=None):
        if system_prompt:
            response = openai.chat.completions.create(
                model="gpt-4",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": prompt}],
                stream=False,
                temperature=0.0,
                top_p=None,
                frequency_penalty=0.0,
                presence_penalty=0.0,
            )
        else:
            response = openai.chat.completions.create(
                model="gpt-4",
                messages=[{"role": "user", "content": prompt}],
                stream=False,
                temperature=0.0,
                top_p=None,
                frequency_penalty=0.0,
                presence_penalty=0.0,
            )
        return response.choices[0].message.content


class Generation(LLM):

    def __init__(self, database_schemas: any, database_name='db_sample'):
        super().__init__()
        self.database_schemas = database_schemas
        self.database_name = database_name

    def data_collection(self, question):
        sql = self.sql_generation_using_gpt(question)
        df = self.read_data(sql)
        return df.to_markdown(), df, sql

    def sql_generation_using_gpt(self, question):
        prompt_generate_sql = dedent("""
        Considering factors need to analysis to answer the user question.
        Here are some requirements:
            - You should always think about what to do.
            - Generate one sqlite sql that fulfilled the factors from the database describe below.
            - The returned data is detailed and related to user question.
            - This sql generation should be enclosed with <text> and </text> tag.
        Note that only use the database below to generate sql.
        The database:
           {database_schemas}
      """)
        p = prompt_generate_sql.format( database_schemas=self.database_schemas)

        @retry(tries=5)
        def run():
            response = self.generate2(question, p, use_llm="GPT4-v2")
            print(response)
            sql = _extract_tag_content(response, 'text')
            print(sql)
            self.read_data(sql)
            return sql

        return run()

    def read_data(self, sql):
        con = Database(
            os.environ.get("SQL_HOST", "localhost"),
            os.environ.get("SQL_USER", "user"),
            os.environ.get("SQL_PASSWORD", "password"),
            os.environ.get("SQL_PORT", "5432"),
            os.environ.get("SQL_DATABASE", "database"))
        # self.db.select_rows_dict
        df = con.select_rows_dict(sql)
        new_df = pd.DataFrame()
        for col in df.columns:
            new_df[col.replace('.', '_')] = df[col]
        print("Data: ", new_df.to_markdown())
        return new_df

    def chart_generation_df_markdown(self, question, df_markdown: str):
        df = pd.read_csv(
            StringIO(df_markdown.replace(' ', '')),  # Get rid of whitespaces
            sep='|',
            index_col=1
        ).dropna(
            axis=1,
            how='all'
        ).iloc[1:]
        # new_df_markdown = '\n'.join(df_markdown.split('\n')[:8])
        # print(new_df_markdown)
        prompt_chart = dedent('''
    Answer the user question by creating vega-lite specification in JSON string.
    First, explain all steps to fulfill the user question.
    Second, here are some requirements:
      1. The data property must be excluded.
      2. You should exclude filters should be applied to the data.
      3. You should consider to aggregate the field if it is quantitative.
      4. You should choose mark type appropriate to user question, the chart has a mark type of bar, line, area, scatter or arc.
      5. The available fields in the dataset and their types are: {head_part}.
    Finally, generate the vega-lite JSON specification between <JSON> and </JSON> tag.
    Lets think step by step.
    ''').format(head_part=str(df.head(5).to_markdown()))

        # print(prompt_chart)

        @retry(tries=5)
        def run():
            config = self.generate2(question, prompt_chart, use_llm="GPT-4")
            print(config)
            _config = _extract_tag_content(config, 'JSON')
            # return _config

            j_config = json.loads(_config)
            if "data" in j_config:
                del j_config["data"]
            spec = copy.deepcopy(j_config)
            spec["data"] = to_values(df)
            print(spec["data"])
            print(json.dumps(spec))
            chart = alt.Chart.from_dict(spec)

            return chart, json.dumps(spec)

        return run()

    def chart_generation_df(self, question, df):
        prompt_chart = dedent('''
            Answer the user question by creating vega-lite specification in JSON string.
            First, explain all steps to fulfill the user question.
            Second, here are some requirements:
              1. The data property must be excluded.
              2. You should exclude filters should be applied to the data.
              3. You should consider to aggregate the field if it is quantitative.
              4. You should choose mark type appropriate to user question, the chart has a mark type of bar, line, area, scatter or arc.
              5. The available fields in the dataset and their types are: {head_part}.
            Finally, generate the vega-lite JSON specification between <JSON> and </JSON> tag.
            Lets think step by step.
            ''').format(head_part=str(df.head(5).to_markdown()))

        # print(prompt_chart)

        @retry(tries=5)
        def run():
            config = self.generate2(question, prompt_chart, use_llm="GPT-4")
            print(config)
            _config = _extract_tag_content(config, 'JSON')
            # return _config

            j_config = json.loads(_config)
            if "data" in j_config:
                del j_config["data"]
            spec = copy.deepcopy(j_config)
            spec["data"] = to_values(df)
            print(spec["data"])
            print(json.dumps(spec))
            chart = alt.Chart.from_dict(spec)

            return chart, json.dumps(spec)

        return run()

    def data_insights_generation(self, question, sql):
        df = self.read_data(sql)
        prompt_data_insight = dedent("""
    Generate 5 bullet points and insights about the data to fullfill user question using belows context. Always answer with the markdown formatting and the crucial information should be colored.

    Context:
    - Data: {data}.

    User question delimited by <>.

    <{question}>
    """).format(question=question, data=df.to_markdown())

        @retry(tries=5)
        def run():
            response = self.generate(prompt_data_insight, use_llm='GPT4')
            print(response)
            return response

        return run()

    def data_insights_generation_1(self, question, df_markdown):
        prompt_data_insight = dedent("""
    Generate 5 bullet points and insights about the data to fullfill user question using belows context. Always answer with the markdown formatting and the crucial information should be colored.

    Context:
    - Data: {data}.

    User question delimited by <>.

    <{question}>
    """).format(question=question, data=df_markdown)

        @retry(tries=5)
        def run():
            response = self.generate(prompt_data_insight, use_llm='GPT4')
            print(response)
            return response

        return run()

    def data_insights_generation_and_answer_question(self, question, df_markdown):
        data_insight = self.data_insights_generation_1(question, df_markdown)

        prompt_answer_question = dedent("""
    Answer user question by using belows contexts. The answer should be explained, always answer with the markdown formatting and the explanation should be colored.

    Context:
      Dataframe:
        {data}
      Data insights information:
        {data_insights}

    User question delimited by <>.

    <{question}>
    """).format(question=question, data=df_markdown, data_insights=data_insight)

        @retry(tries=5)
        def run():
            response = self.generate(prompt_answer_question, use_llm='GPT4')
            print(response)
            return response

        res = run()
        return dedent(f"""
        {data_insight}
        {res}
        """)

    def chart_insights_generation(self, question, config):
        prompt_data_insight = dedent("""
    Generate 5 bullet points and insights about the chart to fullfill user question using belows context. Always answer with the markdown formatting and and the crucial information should be colored.

    Context:
    - Chart: {data}.

    User question delimited by <>.

    <{question}>
    """).format(question=question, data=config)

        @retry(tries=5)
        def run():
            response = self.generate(prompt_data_insight, use_llm='GPT4')
            print(response)
            return response

        return run()

    def answer_data_anlysis_using_context(self, question, df, data_insights, chart_insights):
        prompt_answer_question = dedent("""
    Answer user question by using belows contexts. The answer should be explained, always answer with the markdown formatting and the explanation should be colored.

    Context:
      Dataframe:
        {data}
      Data insights information:
        {data_insights}
      Chart insights information:
        {chart_insights}

    User question delimited by <>.

    <{question}>
    """).format(question=question, data=df.to_markdown(), data_insights=data_insights, chart_insights=chart_insights)

        @retry(tries=5)
        def run():
            response = self.generate(prompt_answer_question, use_llm='GPT4')
            print(response)
            return response

        return run()

    def answer_data_analysis_using_context_1(self, question, df_markdown, data_insights, chart_insights):
        prompt_answer_question = dedent("""
    Answer user question by using belows contexts. The answer should be explained, always answer with the markdown formatting and the explanation should be colored.

    Context:
      Dataframe:
        {data}
      Data insights information:
        {data_insights}
      Chart insights information:
        {chart_insights}

    User question delimited by <>.

    <{question}>
    """).format(question=question, data=df_markdown, data_insights=data_insights, chart_insights=chart_insights)

        @retry(tries=5)
        def run():
            response = self.generate(prompt_answer_question, use_llm='GPT4')
            print(response)
            return response

        return run()

    def data_visualization_df_markdown(self, question, df_markdown):
        _, config = self.chart_generation_df_markdown(question, df_markdown)
        return config

    def data_visualization_df_sql(self, question, sql, limit=20):
        df = self.read_data(sql)
        _df = df[:limit]
        _, config = self.chart_generation_df(question, _df)
        return config

    def data_visualization_with_df(self, question, df):
        _, config = self.chart_generation_df(question, df)
        insights = self.chart_insights_generation(question, config)

        return config, insights

    def data_analysis_1_df_markdown(self, question, df_markdown):
        insights = self.data_insights_generation_1(question, df_markdown)
        return insights

    def data_analysis_2_vega_json(self, question, vega_json):
        insights = self.chart_insights_generation(question, vega_json)
        return insights

    def data_analysis_3(self, question, df_markdown, vega_json):
        data_insights = self.data_insights_generation_1(question, df_markdown)
        chart_insights = self.chart_insights_generation(question, vega_json)
        ans = self.answer_data_analysis_using_context_1(question, df_markdown, data_insights, chart_insights)
        return ans
