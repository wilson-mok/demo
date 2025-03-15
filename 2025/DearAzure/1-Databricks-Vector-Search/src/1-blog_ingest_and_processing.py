# Databricks notebook source
# MAGIC %md
# MAGIC # References
# MAGIC - Ingestion
# MAGIC   - https://learn.microsoft.com/en-us/azure/databricks/ingestion/file-upload/download-internet-files
# MAGIC   - https://learn.microsoft.com/en-us/azure/databricks/files/unzip-files
# MAGIC
# MAGIC - Parsing MD
# MAGIC   - https://www.honeybadger.io/blog/python-markdown/

# COMMAND ----------

# import requests
# import zipfile
# import io

# # Download the zip file from the internet
# url = "https://github.com/Schiiss/blog/archive/refs/heads/master.zip"
# response = requests.get(url)

# # Extract the zip file into a volume
# with zipfile.ZipFile(io.BytesIO(response.content)) as z:
#     z.extractall("/Volumes/bu_ops/project_blog_insight/blog_volume/Schiiss/")

# Show the data in Catalog

# COMMAND ----------

# MAGIC %pip install markdown beautifulsoup4 python-frontmatter
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# Defining UC variables

catalog = "bu_ops"
schema = "project_blog_insight"

data_path = f"/Volumes/{catalog}/{schema}/blog_volume/Schiiss/blog-master/_posts"

table_name = f"{catalog}.{schema}.blog_schiiss"

# COMMAND ----------

# Clean up table if required
spark.sql(f"DROP TABLE IF EXISTS {table_name}")

# COMMAND ----------

# Better understand the structure of the Markdown file. The MD file is strcutured in 2 sections:
#   1. Metadata: title, date, categories, tags. This section is defined by the --- lines
#   2. Content: After the --- lines, the article content. You can see empty lines in between paragraphs. 
# 
# Link: https://schiiss.github.io/blog/genai/ai-safety-in-rag/

demo_md_file = f"{data_path}/2024-11-19-ai-safety-in-rag.md"

demo_df = spark.read.text(demo_md_file)
display(demo_df.limit(30))

# COMMAND ----------

# DBTITLE 1,UDFs to prase the Markdown file
import re
import yaml
import markdown
import frontmatter
from bs4 import BeautifulSoup

from pyspark.sql.functions import *
from pyspark.sql.types import *

# UDF: Extract Markdown metadata using frontmatter
@udf(returnType=StructType([
    StructField("title", StringType(), True),
    StructField("date", TimestampType(), True),
    StructField("categories", ArrayType(StringType()), True),
    StructField("tags", ArrayType(StringType()), True)
]))
def extract_md_metadata_udf(md_content):
    post = frontmatter.loads(md_content)  # Parse frontmatter
    metadata_dict = {
        "title": post.get("title", ""),
        "date": post.get("date", None),
        "categories": post.get("categories", []),
        "tags": post.get("tags", []),
    }
    return metadata_dict

# Parse the MD content. Each line creates a new row.
@udf(returnType=ArrayType(StructType([
    StructField("html_tag", StringType(), True),
    StructField("content", StringType(), True)
])))
def parse_md_content_udf(md_text): 
    
    md_body = re.sub(r"^---\n.*?\n---", "", md_text, flags=re.DOTALL)  # Remove front matter
    md_body = re.sub(r"\{%\s*raw\s*%\}|\{%\s*endraw\s*%\}", "", md_body)  # Remove raw tags

    html = markdown.markdown(md_body)
    soup = BeautifulSoup(html, "html.parser")
    
    content = []
    prev_tag = None # When using ul or li, the 'p' element tag is created. Using this to skip the 'p' element
    for element in soup.find_all(["h1", "h2", "h3", "p", "ul", "li", "code"]):
        ele_text = element.text.strip()
        if len(ele_text) > 0: 
            if not (element.name == "p" and prev_tag in ["li", "ul"]):
                content.append((element.name, ele_text))  # Store tag and content
            prev_tag = element.name
    
    return content

# COMMAND ----------

# DBTITLE 1,Read in the MD data
# List files in the specified volume
files = dbutils.fs.ls(data_path)

# Create a row for each MD file's content
markdown_files = [file.path for file in files if file.path.endswith(".md")]
markdown_df = spark.read.text(markdown_files, wholetext=True) \
                    .withColumn("file_path", col("_metadata.file_path")) \
                    .withColumn("file_name", col("_metadata.file_name")) \
                    .withColumnRenamed("value", "md_content")

display(markdown_df.limit(10))


# COMMAND ----------

# DBTITLE 1,Process the MD data
# Apply UDFs to the DataFrame
processed_df = markdown_df \
                .withColumn("md_metadata", extract_md_metadata_udf(col("md_content"))) \
                .withColumn("parsed_content", parse_md_content_udf(col("md_content")))
processed_df = processed_df.withColumn("parsed_content", explode(col("parsed_content")))

# Create an unique ID for Vector Search
processed_df = processed_df.withColumn("row_id", monotonically_increasing_id())

processed_df = processed_df.select(
                    col("row_id"),
                    col("md_metadata.title").alias("title"),
                    col("md_metadata.date").alias("date"),
                    col("md_metadata.categories").alias("categories"),
                    col("md_metadata.tags").alias("tags"),
                    col("parsed_content.html_tag").alias("html_tag"),
                    col("parsed_content.content").alias("content"),
                    "file_path",
                    "file_name"
)

# Delta Change Data Feed is a requirement to use Vector Search! 
processed_df.write.mode("overwrite").option("delta.enableChangeDataFeed", "true").saveAsTable(table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM bu_ops.project_blog_insight.blog_schiiss
# MAGIC WHERE title like 'AI Safety in RAG%'
# MAGIC LIMIT 50

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT max(len(content))
# MAGIC FROM bu_ops.project_blog_insight.blog_schiiss;