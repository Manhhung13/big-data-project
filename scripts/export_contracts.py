"""Export schema contract JSON without importing/installing PySpark.
Source of truth is `src/stock_batch/schemas.py` field definitions.
"""
from pathlib import Path
import ast
import json
root=Path(__file__).resolve().parents[1]
tree=ast.parse((root/'src/stock_batch/schemas.py').read_text())
fields={}
for node in ast.walk(tree):
    if isinstance(node,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='fields' for t in node.targets):
        fields=ast.literal_eval(node.value)
        break
if not fields:raise RuntimeError('no Spark schema definitions found')
for name,cols in fields.items():
    doc={'dataset':name,'schema_version':'1.0','format':'parquet',
       'spark_types':{'s':'STRING','i':'INT','l':'BIGINT','d':'DOUBLE','b':'BOOLEAN',
       't':'TIMESTAMP','date':'DATE','money':'DECIMAL(20,4)','bin':'BINARY',
       'arr':'ARRAY<STRING>','map':'MAP<STRING,STRING>'},
       'fields':[{'name':k,'type_code':v} for k,v in cols]}
    (root/'contracts/schemas'/f'{name}.json').write_text(json.dumps(doc,ensure_ascii=False,indent=2)+'\n')
print('exported',len(fields),'schema contracts')
