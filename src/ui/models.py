# class DatetimeModel(BaseModel):
#   datetime: arrow.Arrow

#   @validator('datetime', pre=True)
#   def parse_datetime(cls, v):
#     if isinstance(v, str):
#       for parsing_format in ():
#         try:
#                 return arrow.get(v, parsing_format)
#         except xx:
#             pass
#     return v
