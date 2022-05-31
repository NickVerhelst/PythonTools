# General code for testing, needs to be reformatted to be correct.

from avro.codecs import NullCodec
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import date




schema = avro.schema.parse(open("user.avsc","rb").read())

#generate the Avro file
write = DataFileWriter(open("user.avro","wb"), DatumWriter(),schema)
write.append({"name": "Yang", "birthday":date.today(), "workyear": 10})
write.append({"name": "Jason", "birthday":date.today(), "workyear": 1})
write.append({"birthday":date.today(),"name": "David",  "workyear": 4})
write.append({"birthday":date.today(),"name": "Enoch"})
write.close()

""" 
{'name': 'Yang', 'birthday': datetime.date(2021, 5, 3), 'workyear': 10}
{'name': 'Jason', 'birthday': datetime.date(2021, 5, 3), 'workyear': 1}
{'name': 'David', 'birthday': datetime.date(2021, 5, 3), 'workyear': 4}
{'name': 'Enoch', 'birthday': datetime.date(2021, 5, 3), 'workyear': None} 
"""

#read the Avro file and deserialization
reader = DataFileReader(open("user.avro","rb"), DatumReader())
for employee in reader:
    print(employee)
reader.close

#print the Avro schema
reader = avro.datafile.DataFileReader(open('user.avro',"rb"),avro.io.DatumReader())
records = [r for r in reader]
schema = reader.meta
print(schema) 

#{'avro.codec': b'null', 'avro.schema': b'{"type": "record", "name": "employee", "namespace": "com.yanggao365.Lab.Avro", "fields": [{"type": "string", "name": "name"}, {"type": {"type": "int", "logicalType": "date"}, "name": "birthday"}, {"type": ["int", "null"], "name": "workyear"}]}'}

#generate the parquet file
df = pd.DataFrame.from_records(records)
table = pa.Table.from_pandas(df)
pq.write_table(table,'user.parquet')

#print the Parquet file schema
parquet_file = pq.ParquetFile('user.parquet')
print(parquet_file.schema)

""" required group field_id=0 schema {
  optional binary field_id=1 name (String);
  optional int32 field_id=2 birthday (Date);
  optional double field_id=3 workyear;
} """

#print the Parquet data content
table2 = pq.read_table('user.parquet')
print(table2.to_pandas())

"""     
name    birthday  workyear
0   Yang  2021-05-03      10.0
1  Jason  2021-05-03       1.0
2  David  2021-05-03       4.0
3  Enoch  2021-05-03       NaN 
"""