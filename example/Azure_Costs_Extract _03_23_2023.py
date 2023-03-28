# Databricks notebook source
import base64
import json
import requests
from datetime import datetime
from json import JSONDecodeError
from typing import Any, Dict, List, Tuple

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, lit, row_number, upper, when
from empower import Empower
from empower.source import SourceBuilder




# COMMAND ----------

# MAGIC 
# MAGIC %pip install azure-mgmt-billing

# COMMAND ----------

# MAGIC %pip install requests

# COMMAND ----------



# COMMAND ----------

dbutils.widgets.text('fields_query_part','','fields_query_part')
dbutils.widgets.text('subscriptionId','','subscriptionId')
dbutils.widgets.text('object','','object')
dbutils.widgets.text('save_to_path','','save_to_path')
dbutils.widgets.text('kv_secret','','kv_secret')
dbutils.widgets.text('storage_account','','storage_account')
dbutils.widgets.text('storage_container','','storage_container')
dbutils.widgets.text("watermark_date","1900-01-01T00:00:00.000Z","watermark_date")

# COMMAND ----------

dbutils.widgets.text("source_type", "", "source_type")
dbutils.widgets.text("keyvault_secret", "", "keyvault_secret")
dbutils.widgets.text("get_metadata", "", "get_metadata")
dbutils.widgets.text("output_path", "", "output_path")
dbutils.widgets.text("object_name", "", "object_name")
dbutils.widgets.text("cert_path", "", "cert_path")
dbutils.widgets.text("columns", "", "columns")
dbutils.widgets.text("options", "", "options")

# COMMAND ----------

fields_query_part = dbutils.widgets.get('fields_query_part')
object = dbutils.widgets.get('object')
save_to_path = '/mnt/RAW/' + dbutils.widgets.get('save_to_path')
storage_account = dbutils.widgets.get('storage_account')
key_vault_secret_name = dbutils.widgets.get('kv_secret')
storage_container = dbutils.widgets.get('storage_container')
kv_value = dbutils.secrets.get(scope="primary-key-vault-scope",key=key_vault_secret_name)
#kv_value = json.loads(kv_value)
#adl_access_key = kv_value['adl_access_key']
#table_path = kv_value['folder_path'] + '/Tables'

# COMMAND ----------

GET https://management.azure.com/%7Bscope%7D/providers/Microsoft.Consumption/budgets/%7BbudgetName%7D?api-version=2023-03-01
Authorization: Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6Ii1LSTNROW5OUjdiUm9meG1lWm9YcWJIWkdldyIsImtpZCI6Ii1LSTNROW5OUjdiUm9meG1lWm9YcWJIWkdldyJ9.eyJhdWQiOiJodHRwczovL21hbmFnZW1lbnQuY29yZS53aW5kb3dzLm5ldCIsImlzcyI6Imh0dHBzOi8vc3RzLndpbmRvd3MubmV0L2U4NWZlYWRmLTExZTctNDdiYi1hMTYwLTQzYjk4ZGNjOTZmMS8iLCJpYXQiOjE2ODAwMjY2NTQsIm5iZiI6MTY4MDAyNjY1NCwiZXhwIjoxNjgwMDMwOTE5LCJhY3IiOiIxIiwiYWlvIjoiQVZRQXEvOFRBQUFBaVNaU1hUVlB6Ry9yOGR4WXZscktERGc1UGY0ODQrbXU5QUFkVTZ6RVIxeld0WWlFK1gwQnZheS9YZ2tteHkzelZDZVlKdkpBOFFaKzBJNnBuNnpxcHRVY0p6cUFKNmErbjkydTZLeEZHeTQ9IiwiYW1yIjpbInJzYSIsIm1mYSJdLCJhcHBpZCI6IjE4ZmJjYTE2LTIyMjQtNDVmNi04NWIwLWY3YmYyYjM5YjNmMyIsImFwcGlkYWNyIjoiMCIsImRldmljZWlkIjoiMTkyMmI1NzctMDQ5MS00NjhiLTgwNTYtZmVkNjg3MzU1YTc2IiwiZmFtaWx5X25hbWUiOiJMZXZhbnQiLCJnaXZlbl9uYW1lIjoiS2lyaWxsIiwiZ3JvdXBzIjpbIjU2MDFlZjAxLWI1ODMtNDhjMC1hMGYyLTRlYzJjZDkxY2I0YSIsImFhOTE1NTAzLWJmNDItNDg0Ny05OTM1LTczNDMxM2IyNjU5MyIsImM3NmI3NTA3LWUzNTgtNGJiNi1hN2YyLTU4NDAzZWVjZTQ2YSIsIjA3OGNkMzBhLWViNDAtNDg5Ny04YWI2LTRiNWQzYjgzYjQ3MyIsImRiMGViZDBjLTg4NTItNGRkZi1iNGIwLTMzZTAyODVmYmU1MiIsImQxMGIzYjMzLWYzMmUtNGZjOS05MmIyLWVjMzczMzQ3ZmQ0YSIsImM5ZTEyMDM4LWM2ZGMtNGRlNC05YWMxLTc1MDUxNWI3NWM5NyIsImMxMjFmMTNiLWRmZGEtNDZmNC05NjU4LWJjMmNiMDhiMjI5NCIsImI1OWYxZTQ2LTRlZjMtNDI5ZS1hNGMwLTAyNDQ5ZjUxNjI4NyIsIjRhYmQzMjRjLWVkODYtNGNkZS1hYjg5LTlhNDYxNjM5YzAwYSIsImIxYWUxMzRlLTRjOGYtNDQ0Ny05N2EwLWNjODkwNmJhMzEwNSIsImNjYThlNTUxLTRiYzUtNGJiOS1hNGJjLWYyNmU5ZDcyMWVhNSIsIjBiZmU3MTU3LTE5ZGQtNGVjMi1hOWE5LWFmYWFiM2YzMDQ4OCIsIjBjZDI2ZjY3LTc3NjUtNDM4Zi1iN2JhLWJkYjFlMjc5NTEwMSIsIjhkZjIwYzZmLWQ0MTUtNDI0ZC05ODY4LWI1NDU3MmM5NzUzMyIsIjNhNGQyYzczLTUxZmYtNGRhMy1hNDhkLTQ4MTNmZTEwMWRlNCIsIjlhMDBkNDdjLWVmNGEtNDc5YS1iODIwLTQwZDgyZjBjZTIyZiIsImRlMzNmMzg5LTUyOTUtNDc2Ni05YmQxLTVhMzkyNTIzOTgxNSIsIjMwZDVjMzkwLWUwZTctNDc0ZS1iYzRhLTI4NTNkMTRhZGZhNSIsImRmNjg1YTk0LWY0YjQtNDMzZS1hZjFmLTYzMjM2MmUwMjA0NiIsIjMzOTFjOWEyLTJkNjAtNDg0MS1iOGRhLTdiYjBmMjEzMTE3YyIsIjcwZjljOGE3LTU3MjQtNDNkNS1hYzcyLTcwNGNiNGI2OTAwMCIsImJkMjUxMWIzLTVhZGItNDQ0Zi1iMjdhLTE3YWY4ZjBjMGRjNSIsIjY1NjQ2OGIzLWJkY2MtNGE1Ny1hM2NiLThiNDY2OTFhN2EzNSIsImY0M2VlMWI5LWFiNmQtNDUzYS1iMGU4LWYxNzEzNjk3ZGFjYyIsIjI2NGI1ZWU3LTAxNTUtNDliNS04YTU3LTdmNTU4MDUxYWIzNCIsImMyZDRiNmVjLWEzNjUtNGUwMS1iNDEyLTNmOTBjMWRhZGY1ZiIsImYyYWI1ZmYwLTU5NzYtNDIxYS04YmExLTMxOTk0OTQ4MjlkZSIsIjNjODdiOGYwLWFkYWQtNDBhOS05MGVjLTA3OWRjY2EzZjExZSIsImNmNjEwMGY0LWExZDctNDM0Ni1hNWUxLWIxMzQ0ZWE5YzYwNSIsIjVkZGY5M2ZjLTc5ZjQtNGZkOS1hMDgzLTM4MDc3MTQ2ODVlNiIsIjIwNWZkM2ZlLWRmYTMtNGFjNC1iYzM2LTMwNDk5YWRmNzA0NyJdLCJpcGFkZHIiOiIxNzMuMTcxLjY1LjE0MCIsIm5hbWUiOiJLaXJpbGwgTGV2YW50Iiwib2lkIjoiMjZjNjEzN2EtNDQ1Mi00Njg2LTljNmItNGJkMjUzMzJhMmVhIiwib25wcmVtX3NpZCI6IlMtMS01LTIxLTMxNjcxMTQ3NS0zOTUyMDIzNTM2LTM5NDkyNTk2NTItMTg0ODA1IiwicHVpZCI6IjEwMDMyMDAxQzgwMEIzQjUiLCJyaCI6IjAuQVFRQTMtcGY2T2NSdTBlaFlFTzVqY3lXOFVaSWYza0F1dGRQdWtQYXdmajJNQk1FQUp3LiIsInNjcCI6InVzZXJfaW1wZXJzb25hdGlvbiIsInN1YiI6IkRvRnRfUG5FTDNRakxvMC13TUxqZlpTRldCbEVBOVdTSG5mSzlPTlE4aDAiLCJ0aWQiOiJlODVmZWFkZi0xMWU3LTQ3YmItYTE2MC00M2I5OGRjYzk2ZjEiLCJ1bmlxdWVfbmFtZSI6ImtsZXZhbnRAaGl0YWNoaXNvbHV0aW9ucy5jb20iLCJ1cG4iOiJrbGV2YW50QGhpdGFjaGlzb2x1dGlvbnMuY29tIiwidXRpIjoibTBteTczWmstRXE4dEJRLUtKMzNBQSIsInZlciI6IjEuMCIsIndpZHMiOlsiYjc5ZmJmNGQtM2VmOS00Njg5LTgxNDMtNzZiMTk0ZTg1NTA5Il0sInhtc190Y2R0IjoxMzMxOTAzOTU3fQ.Syzkw04AJxiWwJ684gY-Ks4UEDDw8GIe4vx6NlmtnAU9UhkMmNooY93OYCSAf7j1FxSlZTR_BPKrZLe9YxaT-txtvKpKX8mncASFfrEpXY_4RXYDwg18qk_Ca5niY-JsHgdh2Zruwc7tSDOAcvd1zWOYx4KvkxEoQw6Gr8L8lxCcszSo04XA-AmgnlMl5r9cmYVF0xqbtYwfy-C57hAmXr8I2KxA5NigGijrOEiYJIK5WjnBZWl36CoX61FHZ6s0bsyTMXklm7NWvu9zdulQZJ7AGeRaxeiW0ryMyiH3WuFbGBne-MWoNmMh_xCF9gQmEPKCAv-lp3BP-x2yCJ0W6A

# COMMAND ----------

private_access_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6Ii1LSTNROW5OUjdiUm9meG1lWm9YcWJIWkdldyIsImtpZCI6Ii1LSTNROW5OUjdiUm9meG1lWm9YcWJIWkdldyJ9.eyJhdWQiOiJodHRwczovL21hbmFnZW1lbnQuY29yZS53aW5kb3dzLm5ldCIsImlzcyI6Imh0dHBzOi8vc3RzLndpbmRvd3MubmV0L2U4NWZlYWRmLTExZTctNDdiYi1hMTYwLTQzYjk4ZGNjOTZmMS8iLCJpYXQiOjE2ODAwMjY2NTQsIm5iZiI6MTY4MDAyNjY1NCwiZXhwIjoxNjgwMDMwOTE5LCJhY3IiOiIxIiwiYWlvIjoiQVZRQXEvOFRBQUFBaVNaU1hUVlB6Ry9yOGR4WXZscktERGc1UGY0ODQrbXU5QUFkVTZ6RVIxeld0WWlFK1gwQnZheS9YZ2tteHkzelZDZVlKdkpBOFFaKzBJNnBuNnpxcHRVY0p6cUFKNmErbjkydTZLeEZHeTQ9IiwiYW1yIjpbInJzYSIsIm1mYSJdLCJhcHBpZCI6IjE4ZmJjYTE2LTIyMjQtNDVmNi04NWIwLWY3YmYyYjM5YjNmMyIsImFwcGlkYWNyIjoiMCIsImRldmljZWlkIjoiMTkyMmI1NzctMDQ5MS00NjhiLTgwNTYtZmVkNjg3MzU1YTc2IiwiZmFtaWx5X25hbWUiOiJMZXZhbnQiLCJnaXZlbl9uYW1lIjoiS2lyaWxsIiwiZ3JvdXBzIjpbIjU2MDFlZjAxLWI1ODMtNDhjMC1hMGYyLTRlYzJjZDkxY2I0YSIsImFhOTE1NTAzLWJmNDItNDg0Ny05OTM1LTczNDMxM2IyNjU5MyIsImM3NmI3NTA3LWUzNTgtNGJiNi1hN2YyLTU4NDAzZWVjZTQ2YSIsIjA3OGNkMzBhLWViNDAtNDg5Ny04YWI2LTRiNWQzYjgzYjQ3MyIsImRiMGViZDBjLTg4NTItNGRkZi1iNGIwLTMzZTAyODVmYmU1MiIsImQxMGIzYjMzLWYzMmUtNGZjOS05MmIyLWVjMzczMzQ3ZmQ0YSIsImM5ZTEyMDM4LWM2ZGMtNGRlNC05YWMxLTc1MDUxNWI3NWM5NyIsImMxMjFmMTNiLWRmZGEtNDZmNC05NjU4LWJjMmNiMDhiMjI5NCIsImI1OWYxZTQ2LTRlZjMtNDI5ZS1hNGMwLTAyNDQ5ZjUxNjI4NyIsIjRhYmQzMjRjLWVkODYtNGNkZS1hYjg5LTlhNDYxNjM5YzAwYSIsImIxYWUxMzRlLTRjOGYtNDQ0Ny05N2EwLWNjODkwNmJhMzEwNSIsImNjYThlNTUxLTRiYzUtNGJiOS1hNGJjLWYyNmU5ZDcyMWVhNSIsIjBiZmU3MTU3LTE5ZGQtNGVjMi1hOWE5LWFmYWFiM2YzMDQ4OCIsIjBjZDI2ZjY3LTc3NjUtNDM4Zi1iN2JhLWJkYjFlMjc5NTEwMSIsIjhkZjIwYzZmLWQ0MTUtNDI0ZC05ODY4LWI1NDU3MmM5NzUzMyIsIjNhNGQyYzczLTUxZmYtNGRhMy1hNDhkLTQ4MTNmZTEwMWRlNCIsIjlhMDBkNDdjLWVmNGEtNDc5YS1iODIwLTQwZDgyZjBjZTIyZiIsImRlMzNmMzg5LTUyOTUtNDc2Ni05YmQxLTVhMzkyNTIzOTgxNSIsIjMwZDVjMzkwLWUwZTctNDc0ZS1iYzRhLTI4NTNkMTRhZGZhNSIsImRmNjg1YTk0LWY0YjQtNDMzZS1hZjFmLTYzMjM2MmUwMjA0NiIsIjMzOTFjOWEyLTJkNjAtNDg0MS1iOGRhLTdiYjBmMjEzMTE3YyIsIjcwZjljOGE3LTU3MjQtNDNkNS1hYzcyLTcwNGNiNGI2OTAwMCIsImJkMjUxMWIzLTVhZGItNDQ0Zi1iMjdhLTE3YWY4ZjBjMGRjNSIsIjY1NjQ2OGIzLWJkY2MtNGE1Ny1hM2NiLThiNDY2OTFhN2EzNSIsImY0M2VlMWI5LWFiNmQtNDUzYS1iMGU4LWYxNzEzNjk3ZGFjYyIsIjI2NGI1ZWU3LTAxNTUtNDliNS04YTU3LTdmNTU4MDUxYWIzNCIsImMyZDRiNmVjLWEzNjUtNGUwMS1iNDEyLTNmOTBjMWRhZGY1ZiIsImYyYWI1ZmYwLTU5NzYtNDIxYS04YmExLTMxOTk0OTQ4MjlkZSIsIjNjODdiOGYwLWFkYWQtNDBhOS05MGVjLTA3OWRjY2EzZjExZSIsImNmNjEwMGY0LWExZDctNDM0Ni1hNWUxLWIxMzQ0ZWE5YzYwNSIsIjVkZGY5M2ZjLTc5ZjQtNGZkOS1hMDgzLTM4MDc3MTQ2ODVlNiIsIjIwNWZkM2ZlLWRmYTMtNGFjNC1iYzM2LTMwNDk5YWRmNzA0NyJdLCJpcGFkZHIiOiIxNzMuMTcxLjY1LjE0MCIsIm5hbWUiOiJLaXJpbGwgTGV2YW50Iiwib2lkIjoiMjZjNjEzN2EtNDQ1Mi00Njg2LTljNmItNGJkMjUzMzJhMmVhIiwib25wcmVtX3NpZCI6IlMtMS01LTIxLTMxNjcxMTQ3NS0zOTUyMDIzNTM2LTM5NDkyNTk2NTItMTg0ODA1IiwicHVpZCI6IjEwMDMyMDAxQzgwMEIzQjUiLCJyaCI6IjAuQVFRQTMtcGY2T2NSdTBlaFlFTzVqY3lXOFVaSWYza0F1dGRQdWtQYXdmajJNQk1FQUp3LiIsInNjcCI6InVzZXJfaW1wZXJzb25hdGlvbiIsInN1YiI6IkRvRnRfUG5FTDNRakxvMC13TUxqZlpTRldCbEVBOVdTSG5mSzlPTlE4aDAiLCJ0aWQiOiJlODVmZWFkZi0xMWU3LTQ3YmItYTE2MC00M2I5OGRjYzk2ZjEiLCJ1bmlxdWVfbmFtZSI6ImtsZXZhbnRAaGl0YWNoaXNvbHV0aW9ucy5jb20iLCJ1cG4iOiJrbGV2YW50QGhpdGFjaGlzb2x1dGlvbnMuY29tIiwidXRpIjoibTBteTczWmstRXE4dEJRLUtKMzNBQSIsInZlciI6IjEuMCIsIndpZHMiOlsiYjc5ZmJmNGQtM2VmOS00Njg5LTgxNDMtNzZiMTk0ZTg1NTA5Il0sInhtc190Y2R0IjoxMzMxOTAzOTU3fQ.Syzkw04AJxiWwJ684gY-Ks4UEDDw8GIe4vx6NlmtnAU9UhkMmNooY93OYCSAf7j1FxSlZTR_BPKrZLe9YxaT-txtvKpKX8mncASFfrEpXY_4RXYDwg18qk_Ca5niY-JsHgdh2Zruwc7tSDOAcvd1zWOYx4KvkxEoQw6Gr8L8lxCcszSo04XA-AmgnlMl5r9cmYVF0xqbtYwfy-C57hAmXr8I2KxA5NigGijrOEiYJIK5WjnBZWl36CoX61FHZ6s0bsyTMXklm7NWvu9zdulQZJ7AGeRaxeiW0ryMyiH3WuFbGBne-MWoNmMh_xCF9gQmEPKCAv-lp3BP-x2yCJ0W6A"

headers = {
    "Authorization": "Bearer " + private_access_token,
    "Content-Type": "application/json"
}
url = "https://management.azure.com/subscriptions/b5f57abf-349d-414a-a5a2-9e560d398a25/providers/Microsoft.Billing/billingPeriods?api-version=2017-04-24-preview"

# API_Call = f'GET https://management.azure.com/subscriptions/b5f57abf-349d-414a-a5a2-9e560d398a25/providers/Microsoft.Billing/billingPeriods?api-version=2017-04-24-preview'

# API_Call_Details = 'GET https://management.azure.com/subscriptions/{subscriptionId}/providers/Microsoft.Consumption/usageDetails?api-version=2018-03-31&$expand=properties/additionalProperties'

# COMMAND ----------

response = requests.get(url=url, headers=headers)
print(response.status_code)
print(response.json())

# COMMAND ----------

class AzureCosts(SourceBuilder):

	REQUEST_CLIENT = requests
	HEADERS = {"Accept": "application/json"}
	POST_BATCH_URL = "/api/now/v1/batch"
	OBJECTS_URL = "/api/now/doc/table/schema"
	META_URL = "/api/now/ui/meta/"
	DATA_URL = "/api/now/table/"
	MAX_RESULTS = 10000
	METADATA_SCHEMA = ["OBJECTSCHEMA", "OBJECTNAME", "ESTIMATEDROWCOUNT", "CANDIDATEKEY",
					   "FIELDNAME", "SOURCEQUERYPART", "MANDATORY", "FIELDTYPE"]

	def __init__(self, kwargs=None):
		super().__init__()
		if kwargs is not None:
			self.options(**kwargs)

	def configure_secrets(self, secret: str):
		""" Parse the key value pairs in the secret into the strategy's configuration. """
		secrets = json.loads(secret)
		self.options(**secrets)

	def validate_attributes(self) -> bool:
		return super().validate_attributes()

	def acquire(self):
		return super().acquire()

	# Metadata Implementation

	def metadata(self) -> Tuple[DataFrame, DataFrame]:
		""" Build the object list and field list metadata sets. """
		# Validate attributes are set
		url = self.__getattribute__("url")
		authentication = (self.__getattribute__("username"), self.__getattribute__("password"))

		# If object not specified, extract metadata for all objects
		if self.object is not None:
			objects = [self.object]
		else:
			objects = self.get_metadata_objects(url, authentication, self.HEADERS)

		object_map, request_batch = self.batch_metadata_requests(objects)
		responses = self.get_batch_responses(url, authentication, self.HEADERS, request_batch)
		metadata = self.parse_metadata(responses)

		# Build dataframe from json responses
		object_map_df = self.spark.session.createDataFrame(object_map, ["id", "object", "url"])
		metadata_df = self.spark.session.createDataFrame(metadata, self.METADATA_SCHEMA)
		df = self.transform_metadata(object_map_df, metadata_df)

		# Select object list and field list
		object_list = self.build_metadata_list("object", df)
		field_list = self.build_metadata_list("field", df)
		self.METRICS.rowsRead = field_list.count()

		return object_list, field_list

	@classmethod
	def batch_metadata_requests(cls, object_list: List[str]) -> Tuple[List[Dict], List[Dict]]:
		""" Build a list of objects and their metadata API URLs. """
		batch_urls = []
		for obj in object_list:
			batch_urls.append({
				"id": str(object_list.index(obj)),
				"object": obj,
				"url": f"{cls.META_URL}{obj}"
			})
		return batch_urls, cls.batch_rest_requests(batch_urls)

	@classmethod
	def get_metadata_objects(cls, client_url: str, authentication: Tuple, headers: Dict) -> List[str]:
		""" Get a list of all objects from which to extract metadata. """
		objects = []
		results = cls.REQUEST_CLIENT.get(f"https://{client_url}{cls.OBJECTS_URL}", auth=authentication, headers=headers)
		results = json.loads(results.content.decode('utf-8')).get('result')

		for result in results:
			objects.append(result.get("value"))

		return objects

	@staticmethod
	def parse_metadata(metadata_objects: List[str]) -> List[List[Any]]:
		""" Parse the attributes out of each metadata object. """
		fields = []
		for s in metadata_objects:
			obj = json.loads(s)
			obj_id = obj.get("id")
			for column in obj.get("columns"):
				attributes = obj.get("columns").get(column)
				name = attributes.get("name")
				data_type = attributes.get("base_type")
				nullable = attributes.get("mandatory")
				fields.append(['servicenow', obj_id, 0, 'sys_id', name, name, nullable, data_type])

		return fields

	@staticmethod
	def transform_metadata(object_map: DataFrame, metadata: DataFrame) -> DataFrame:
		""" Transform the extracted data types to Spark types. """
		df = metadata.join(object_map, metadata.OBJECTNAME == object_map.id, "left")
		df = df.withColumn("OBJECTNAME", col("object"))
		df = df.withColumn("TABLENAME", col("object"))
		df = df.withColumn(
			"FIELDTYPE",
			when(col("FIELDTYPE") == "boolean", lit("bit")).otherwise(
				when(col("FIELDTYPE") == "float", lit("double")).otherwise(
					when(col("FIELDTYPE") == "integer", lit("int")).otherwise(
						col("FIELDTYPE"))))
		)
		df = df.withColumn("NULLABLE", when(upper(col("MANDATORY")) == "FALSE", lit(1)).otherwise(lit(0)))
		window = Window.orderBy("FIELDNAME").partitionBy("OBJECTNAME")
		df = df.withColumn("COLUMNORDINAL", row_number().over(window))

		df = df.drop("id", "object", "url")

		return df

	# Data Extraction Implementation

	def extract(self) -> DataFrame:
		""" Extract data for a ServiceNow table.  """
		# Validate attributes are set
		url = self.__getattribute__("url")
		authentication = (self.__getattribute__("username"), self.__getattribute__("password"))
		columns = self.__getattribute__("columns")
		watermark = self.__getattribute__("watermark")
		start_date = self.__getattribute__("start_date")
		end_date = self.__getattribute__("end_date")

		# Set watermarks
		self.METRICS.oldWaterMarkDate = start_date
		self.METRICS.newWaterMarkDate = start_date

		# Create batch of sysid's to retrieve from table
		batch_ids = self.get_sysid_batch(url, self.object, watermark, start_date, end_date,
										 authentication, self.HEADERS)
		self.METRICS.sourceRowCount = len(batch_ids)

		# Return an empty dataframe if there is no data to extract
		if self.METRICS.sourceRowCount == 0:
			self.METRICS.newWaterMarkDate = end_date
			schema = ",".join([f"{field} STRING" for field in columns.split(",")])
			return self.spark.session.createDataFrame([], schema)

		request_batch = self.batch_extraction_requests(self.object, batch_ids, columns)
		responses = self.get_batch_responses(url, authentication, self.HEADERS, request_batch)
		self.METRICS.rowsRead = len(responses)

		# Clean serviced requests response bodies
		cleaned = []
		for response in responses:
			cleaned.append(ServiceNow.clean_response(response))

		# Build dataframe from json responses
		df, corrupt = self.json_to_dataframe(self.spark.session, self.object, columns, cleaned)
		self.METRICS.rowsSkipped = corrupt.count()

		# Update new watermark
		self.METRICS.newWaterMarkDate = end_date

		return df

	@classmethod
	def get_sysid_batch(cls, client_url: str, table: str, watermark: str, start_date: str, end_date: str,
						authentication: Tuple, headers: Dict) -> List[Dict]:
		""" Return a list of lists of a table's sys_ids with a sys_updated_on value between the watermarks. """
		result = cls._get_sysid(client_url, table, watermark, start_date, end_date,
								authentication, headers).get("result")

		# Response exceeds maximum results, make multiple requests
		if result and len(result) >= cls.MAX_RESULTS:
			# find midpoint of watermark and new watermark
			diff = (datetime.strptime(end_date, "%Y-%m-%d %H:%M:%S") -
					datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S"))
			mid_date = (datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S") +
						(diff / 2)).strftime("%Y-%m-%d %H:%M:%S")

			ids_a = cls.get_sysid_batch(client_url, table, watermark, start_date, mid_date, authentication, headers)
			ids_b = cls.get_sysid_batch(client_url, table, watermark, mid_date, end_date, authentication, headers)
			return ids_a + ids_b
		else:
			return list(result)

	@classmethod
	def _get_sysid(cls, client_url: str, table: str, watermark: str, start_date: str, end_date: str,
				   authentication: Tuple, headers: Dict) -> Dict:
		""" Return a dictionary of a table's sys_ids with a sys_updated_on value between the start and end dates. """
		if watermark is not None:
			url = f"https://{client_url}{cls.DATA_URL}{table}" \
				  f"?sysparm_query={watermark}>{start_date}^{watermark}<={end_date}&sysparm_fields=sys_id"
		else:
			url = f"https://{client_url}{cls.DATA_URL}{table}?sysparm_fields=sys_id"

		response = cls.REQUEST_CLIENT.get(url, auth=authentication, headers=headers)
		return json.loads(response.content.decode('utf-8'))

	@classmethod
	def batch_extraction_requests(cls, table: str, ids: List[Dict], columns: str = None) -> List[Dict]:
		""" Build a list of objects and their Table API URLs. """
		batch_urls = []
		sysparm_fields = [f"&sysparm_fields={columns}" if columns else ""][0]

		for i in ids:
			batch_urls.append({
				"id": ids.index(i),
				"object": table,
				"sys_id": i.get('sys_id'),
				"url": f"{cls.DATA_URL}{table}/{i.get('sys_id')}" +
					   f"?sysparm_exclude_reference_link=true{sysparm_fields}"
			})
		return cls.batch_rest_requests(batch_urls)

	@staticmethod
	def clean_response(body: str) -> Dict:
		""" Replace decoding artifacts that prevent a response string from parallelizing into an RDD. """
		try:
			cleaned = ServiceNow.clean_formatting(body)
			cleaned = json.loads(cleaned)
		except (SyntaxError, JSONDecodeError):
			cleaned = ServiceNow.clean_quotes(body)
			cleaned = ServiceNow.clean_formatting(cleaned)
			cleaned = json.loads(cleaned)

		# Replace None values with an empty string to prevent downstream schema issues
		[cleaned.update({k: ""}) for (k, v) in cleaned.items() if v is None]

		return cleaned

	@staticmethod
	def clean_formatting(body: str) -> str:
		""" Remove decoding character artifacts from string. """
		return body.replace("\\u00a0", "") \
			.replace("\\u200b", "") \
			.replace('\\"', "'")

	@staticmethod
	def clean_quotes(body: str) -> str:
		""" Remove problematic quotation marks from string. """
		return body.replace('\\"', "'") \
			.replace("\\", "/") \
			.replace("/'", '"')

	# Batch API

	@staticmethod
	def batch_rest_requests(batch_urls: [Dict]) -> [Dict]:
		""" Create the REST requests to send to the Batch API """
		batch_requests = []

		for url in batch_urls:
			batch_requests.append({
				"id": str(url.get("id")),
				"exclude_response_headers": "true",
				"headers": [
					{
						"name": "Content-Type",
						"value": "application/json"
					},
					{
						"name": "Accept",
						"value": "application/json"
					}
				],
				"method": "GET",
				"url": url.get("url")
			})
		return batch_requests

	@classmethod
	def get_batch_responses(cls, client_url: str, authentication: Tuple, headers: Dict,
							request_batch: List[Dict]) -> List[str]:
		""" Iterate over a batch of requests until all requests are successful. """
		successful_requests = []
		i = 0
		while len(request_batch) > 0:
			retry_requests = []
			retry_batch = []
			i = i + 1
			responses = cls.post_request_batch(client_url, authentication, headers, request_batch, i)

			for response in responses:
				success, failed = cls.parse_response(response)
				successful_requests = successful_requests + success
				retry_requests = retry_requests + failed

			# Remove successful requests from the request batch
			# Due to missed elements and duplicate calls, a second array is created and reassigned
			for request in request_batch:
				if request.get("id") in retry_requests:
					retry_batch.append(request)
			request_batch = retry_batch

		return successful_requests

	@classmethod
	def post_request_batch(cls, client_url: str, authentication: Tuple, headers: Dict,
						   request_batch: List[Dict], batch_id: int = 1) -> List[Dict]:
		""" Post the batch of requests to the Batch API.
		If the body of a request is too large, split the body in two and retry the requests.
		"""
		url = f"https://{client_url}{cls.POST_BATCH_URL}"
		body = {
			"batch_request_id": batch_id,
			"rest_requests": request_batch
		}
		response = cls.REQUEST_CLIENT.post(url=url, json=body, auth=authentication, headers=headers)

		# Request body is too large, make multiple requests
		if response.status_code == 413:
			half_batch = int(len(request_batch) / 2)
			response_a = cls.post_request_batch(client_url, authentication, headers,
												request_batch[:half_batch], batch_id)
			response_b = cls.post_request_batch(client_url, authentication, headers,
												request_batch[half_batch:], batch_id)
			return response_a + response_b
		else:
			return [json.loads(response.content.decode("utf-8"))]

	@classmethod
	def parse_response(cls, response: Dict) -> Tuple[List[str], List[str]]:
		""" Parse the response from a Batch API request.
		A response has a serviced request property and an unserviced request property.
		Serviced requests are a dictionary with a status code and a body.
		Unserviced requests are a list of the failed request id's.
		"""
		successful_requests = []
		failed_requests = []

		for request in response.get("serviced_requests"):
			if request.get("status_code") == 200:
				body = request.get("body")
				result = json.loads(base64.b64decode(body)).get("result")
				result.update({"id": request.get("id")})
				successful_requests.append(json.dumps(result))
			elif request.get("status_code") == 500:
				failed_requests.append(request.get("id"))

		return successful_requests, failed_requests + response.get("unserviced_requests")

# COMMAND ----------



# COMMAND ----------

fields_query_part = fields_query_part.split(',')
save_to_path = os.path.splitext(save_to_path)[0]
file_drop = save_to_path.replace('/mnt/','/')

# COMMAND ----------

spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net", f"{adl_access_key}")

# COMMAND ----------

# Setup the file system client for ADLS
def set_file_system(adl_account, adl_container, adl_key):
    file_system = FileSystemClient(account_url = f"https://{adl_account}.dfs.core.windows.net/", file_system_name = adl_container, credential = adl_key)
    
    return file_system


# From the file system fetch all the CSVs corresponding to the desired object
# TODO: These are named like Object_0001.csv, but can there ever be more than one?
def get_file_paths(fs, path, obj, adl_account, adl_container):
    ret = []
    
    paths = fs.get_paths(path = path, recursive = True)
    for path in paths:
        if object.upper() + '_' in path.name:
            ret.append(f"abfss://{adl_container}@{adl_account}.dfs.core.windows.net/{path.name}")
            
    return ret

# COMMAND ----------

try:
    file_system = set_file_system(storage_account, storage_container, adl_access_key)
    paths = get_file_paths(file_system, table_path, object, storage_account, storage_container)

    # Read all csvs into a spark DataFrame. Use the schema from the Fields Query
    objectSchema = t.StructType([t.StructField(c, t.StringType()) for c in fields_query_part])
    data = spark.read.csv(paths, header=False, schema=objectSchema, multiLine=True, escape="\"")

    data_count = data.count()
    if data_count > 0:
        # Write to RAW
        data.write.format('parquet').save(save_to_path)
        
        # Get the watermark for future changefeed extracts
        tmp = data.withColumn('watermark_date',f.coalesce(data['LastProcessedChange_DateTime'],data['DataLakeModified_DateTime'])) 
        max_date = tmp.select(f.max('watermark_date')).collect()[0][0]  
        #max_date = csv_final.select(f.max('LastProcessedChange_DateTime')).collect()[0][0]

        try:
            max_date = datetime.strptime(max_date, "%Y-%m-%dT%H:%M:%S.%f0") - timedelta(hours=1)
        except:
            max_date = datetime.strptime(max_date[:26], "%Y-%m-%dT%H:%M:%S.%f") - timedelta(hours=1)
        finally:  
            new_watermark_date = max_date.strftime("%Y-%m-%dT%H:%M:%S.%fZ")    

        output = {
            "sourceRowCount": data_count,
            "rowsRead": data_count,
            "rowsCopied": data_count,
            "errors": None,
            "oldWaterMarkDate": new_watermark_date,
            "newWaterMarkDate": new_watermark_date,
            "extractionSuccess": 1,
            "fileDrop": file_drop
        }
    else:
        output = {
            "sourceRowCount": 0,
            "rowsRead": 0,
            "rowsCopied": 0,
            "errors": None,
            "oldWaterMarkDate": watermark_date,
            "newWaterMarkDate": watermark_date,
            "extractionSuccess": 1,
            "fileDrop": "No source data"
        }
except Exception as e:
    output = {
        "sourceRowCount": None,
        "rowsRead": None,
        "rowsCopied": None,
        "errors": str(e),
        "oldWaterMarkDate": "1900-01-01T00:00:00.000Z",
        "newWaterMarkDate": "1900-01-01T00:00:00.000Z",
        "extractionSuccess": 0,
        "fileDrop": "No file"
    }

# COMMAND ----------

dbutils.notebook.exit(json.dumps(output))
