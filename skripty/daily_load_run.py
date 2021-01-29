
from pyspark.sql import functions as F
from pyspark.sql.functions import *
import matplotlib.dates as mdates
import matplotlib.ticker as tick
import matplotlib.pyplot as plt
import pandas as pd


def create_one_day_report(filename,spark):
	sales = spark.sql("select * from semestralka_lauderdice.sales")
	sales = sales.withColumn("datum", to_date(sales.datum, 'dd.MM.yyyy'))
	sales = sales.withColumn("revenue", sales["item_cnt_day"] * sales["item_price"])
	#sales = sales.filter(col("datum") <= "2013-11-30")
	cats = spark.sql("select * from semestralka_lauderdice.cat")
	items = spark.sql("select * from semestralka_lauderdice.items")
	max_date = sales.groupBy().agg(max("datum")).toDF("lastday")
	max_date = max_date.withColumn("previous", date_sub(max_date["lastday"], 1))
	afterday = max_date.collect()[0][0]
	beforeday = max_date.collect()[0][1]
	sales_one_day = sales.filter(col("datum") == afterday)
	prehled_kategorii = sales_one_day.groupBy("itemcat_id").agg({'item_cnt_day': 'sum', 'revenue': 'sum'}).toDF("itemcat_id",
	                                                                                                    "trzby",
	                                                                                                    "pocet_kusu")
	prehled_kategorii = prehled_kategorii.withColumn("trzby", round(col("trzby"), 2))
	prehled_kategorii = prehled_kategorii.join(cats, cats.cat_id == prehled_kategorii.itemcat_id, 'left_outer')
	prehled_kategorii = prehled_kategorii.withColumn("kategorie", prehled_kategorii["cat_name"]).select("kategorie",
	                                                                                                    "trzby",
	                                                                                                    "pocet_kusu").sort(
	    "trzby", ascending=False)
	top_count_sold = sales_one_day.groupBy("item_id").agg({'item_cnt_day': 'sum'}).toDF("item_id", "pocet_kusu").sort(
	    "pocet_kusu", ascending=False)
	top_count_sold = top_count_sold.join(items, items.item_id == top_count_sold.item_id,
	                                     "left_outer").withColumnRenamed("item_name", "produkt").select("produkt",
	                                                                                                    "pocet_kusu").limit(10)
	top_revenue = sales_one_day.groupBy("item_id").agg({'revenue': 'sum'}).toDF("item_id", "trzby").sort("trzby",
	                                                                                             ascending=False)
	top_revenue = top_revenue.join(items, items.item_id == top_revenue.item_id, "left_outer").withColumnRenamed(
	    "item_name", "produkt").select("produkt", "trzby").withColumn("trzby", round(col("trzby"), 0)).limit(10)
	poschange, negchange = calculate_change(beforeday, afterday, sales)
	final_one_day_output = "Denni report  - "+ str(afterday)+'\n\n'+"-------------------- Prehled kategorii --------------------" + '\n\n' + prehled_kategorii.toPandas().to_string() + '\n\n' + "-------------------- TOP 10 produktu (pocty kusu) --------------------" + '\n\n' + top_count_sold.toPandas().to_string() + '\n\n' + "-------------------- TOP 10 produktu (trzby) --------------------" + '\n\n' + top_revenue.toPandas().to_string() + '\n\n' + "-------------------- TOP 10 produktu (denni zmena +) --------------------" + '\n\n' + poschange.toPandas().to_string() + '\n\n' + "-------------------- TOP 10 produktu (denni zmena -) --------------------" + '\n\n' + negchange.toPandas().to_string()
	wtf(filename, final_one_day_output)



def wtf(filename,text):
	f = open(filename,"w+",encoding = "utf-8")
	f.write(text)
	f.close()


def calculate_change(beforeday,afterday,sales):
	print(beforeday)
	print(afterday)
	groupedsales = sales.groupBy("datum","item_id").agg({'item_cnt_day':'sum'}).toDF("datum","item_id","item_cnt_day")
	salesbef = groupedsales.filter(col("datum") == beforeday).select("item_id","item_cnt_day").withColumnRenamed("item_id", "id")
	salesaft = groupedsales.filter(col("datum") == afterday).select("item_id","item_cnt_day").withColumnRenamed("item_id", "id")
	itemsfirst = salesbef.select("id")
	itemssecond = salesaft.select("id")
	ciselnik = salesbef.select("id").unionAll(salesaft.select("id")).dropDuplicates().withColumnRenamed("id", "item_id")
	salestwodays = ciselnik.join(salesbef,salesbef.id==ciselnik.item_id,"left_outer").withColumnRenamed("item_cnt_day", "count_before").drop('id').join(salesaft,salesaft.id==ciselnik.item_id,"left_outer").withColumnRenamed("item_cnt_day", "count_after").drop('id')
	salestd = salestwodays.na.fill(value=0)
	salestd = salestd.join(items.select("item_id","item_name"),items.item_id == salestd.item_id, how= "left_outer")
	salestd = salestd.withColumn("change",salestd["count_after"] - salestd["count_before"])
	poschange = salestd.filter(col("change")>0).sort("change",ascending = False).limit(10).select("item_name","change").withColumnRenamed("item_name", 	"produkt").withColumnRenamed("change", "denni_zvyseni")
	negchange = salestd.filter(col("change")<0).sort("change",ascending = True).limit(10).select("item_name","change").withColumnRenamed("item_name", "produkt").withColumnRenamed("change", "denni_snizeni")
	return poschange,negchange


create_one_day_report("semestralka_output/example_one_day_report.txt", spark)
