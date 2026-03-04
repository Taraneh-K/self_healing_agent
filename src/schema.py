from pydantic import BaseModel, Field

# class DataFix(BaseModel):
#     """
#     This is the 'contract' the AI must follow. 
#     It forces the LLM to return exactly these two fields.
#     """
#     python_code: str = Field(
#         description="The Polars Python code required to clean the column."
#     )
#     explanation: str = Field(
#         description="A short explanation of why the data was broken and how it was fixed."
#     )

from pydantic import BaseModel, Field, AliasChoices

# class DataFix(BaseModel):
#     # This allows the model to accept either 'python_code' OR 'expression'
#     python_code: str = Field(
#         validation_alias=AliasChoices('python_code', 'expression')
#     )
#     explanation: str = Field(
#         default="No explanation provided by AI."
#     )


class DataFix(BaseModel):
    explanation: str
    python_code: str

class ArticleSchema(BaseModel):
    """
    Optional: Use this to define what a 'Clean' article looks like.
    """
    title: str
    author: str
    published_date: str
    word_count: int