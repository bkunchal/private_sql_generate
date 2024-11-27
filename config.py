queries = { 
        "extract": [
{
               "name": "extract_data",
              "query": """SELECT col1,co12 FROM my_table"""
              }
],
  "transform": [
           {
      "name": "grouped_and_filtered"
      "query": """SELECT col1,
                          co12,
                          SUM (co13) AS total,
                              FROM extract_data
                              GROUP BY coll, col2"""
                 },
                 {
                     "name": "stagging_data",
                     "query": """SELECT coll,
                                        co12,
                                        total from grouped_and_filtered"""
                 }
                 ],
    "load" : [
    {
    "table_name" : "tablename",
    "dataframe_name" : """stagging_data""",
    "load_type" : "overwrite"
                         
    } ]
                 }