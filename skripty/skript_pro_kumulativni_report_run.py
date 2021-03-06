
from pyspark.sql import functions as F
from pyspark.sql.functions import *
import matplotlib.dates as mdates
import matplotlib.ticker as tick
import matplotlib.pyplot as plt
import pandas as pd

sales = spark.sql("select * from semestralka_lauderdice.sales")
sales = sales.withColumn("revenue", sales["item_cnt_day"] * sales["item_price"])
sales = sales.withColumn("datum", to_date(sales.datum, 'dd.MM.yyyy'))
items = spark.sql("select * from semestralka_lauderdice.items")

#ABC ANALYZA

def abc(sales,items):
	def create_abc_showcase(abcxyz):
		abc_string = "ABC"
		xyz_string = "XYZ"
		showcaseabc = abcxyz.filter(col("abc") == "K")
		for abc_letter in abc_string:
			for xyz_letter in xyz_string:
				print(abc_letter, xyz_letter)
				df = abcxyz.filter((col("abc") == abc_letter) & (col("xyz") == xyz_letter)).limit(3)
				showcaseabc = showcaseabc.unionAll(df)
		return showcaseabc
	productrev = sales.groupBy("item_id").agg({"revenue":"sum"}).toDF("item_id","revenue")
	totalrev = productrev.select("revenue").groupBy().sum().collect()[0][0]
	productrev = productrev.withColumn("relative", productrev["revenue"] / totalrev).sort("relative",ascending = False)
	productrev = productrev.withColumn('cumsum', expr('sum(relative) over (order by relative desc)'))
	abc = productrev.withColumn("abc",when(productrev["cumsum"]<=0.8, "A").otherwise(when(productrev["cumsum"]<=0.95, "B").otherwise("C"))).select("item_id","abc")
	abc_count = abc.groupBy("abc").agg({"item_id":"count"}).toDF("abc","count")
	xyz = sales.withColumn('month',month(sales["datum"]))
	xyz = xyz.groupBy("item_id","month").agg({"item_cnt_day":"sum"}).toDF("item_id","month","count_day")
	xyz = xyz.groupBy("item_id").agg(avg("count_day"), stddev("count_day")).toDF("item_id","mean", "sd").na.fill(value=0)
	xyz = xyz.withColumn("varcoeff", xyz["sd"]/xyz["mean"]).select("item_id","varcoeff")
	xyz = xyz.withColumn("xyz", when(xyz["varcoeff"]<=0.1, "X").otherwise(when(xyz["varcoeff"]<=0.25, "Y").otherwise("Z"))).select("item_id","xyz").withColumnRenamed("item_id","item")
	combo = abc.join(xyz,abc.item_id == xyz.item).select("item_id","abc","xyz")
	combo_human = combo.join(items,items.item_id == combo.item_id,"left_outer").select("item_name","abc","xyz").withColumnRenamed("item_name", "Produkt")
	combo_human.toPandas().to_csv("semestralka_output/ABC_abcxyz_fullproducts.csv", index = False, encoding = "UTF-8")
	#to_html("semestralka_output/ABC_abcxyz_fullproducts.html", index = False, encoding = "UTF-8")
	abcxyzcount = combo.groupBy("abc","xyz").count().toDF("ABC skupina","XYZ skupina","pocet produktu").sort("ABC skupina","XYZ skupina")
	abcxyzcount.toPandas().to_csv("semestralka_output/ABC_abcxyz_count.csv", index = False, encoding = "UTF-8")
	#to_html("semestralka_output/abcxyz_count.html", index = False, encoding = "UTF-8")
	showcaseabc = create_abc_showcase(combo)
	showcaseabc = showcaseabc.join(items,items.item_id == showcaseabc.item_id,"left_outer").select("item_name","abc","xyz").withColumnRenamed("item_name", "Produkt")
	showcaseabc.toPandas().to_csv("semestralka_output/ABC_showcase_abcxyz.csv", index = False, encoding = "UTF-8")
	#to_html("semestralka_output/ABC_showcase_abcxyz.html", index = False, encoding = "UTF-8")
	return combo

combo = abc(sales, items)

#ROZDELENI PRUMERNYCH CEN - GRAF 1
def generate_price_distributions(sales, items):
	prcen = sales.groupBy("item_id").agg({"item_price":"avg"}).toDF("item_id","avgprice")
	prcentable = prcen.sort("avgprice",ascending = False).limit(10).withColumnRenamed("item_id", "produkt").withColumn("avgprice",round(prcen["avgprice"],2)).withColumnRenamed("avgprice","Prumerna cena")
	prcentable = prcentable.join(items,items.item_id == prcentable.produkt,"left_outer").select("item_name","item_id","Prumerna cena").withColumnRenamed("item_name", "Produkt")
	prcentable.toPandas().to_csv("semestralka_output/GRAF_1_avg_prices_products.csv", index = False)
	#to_html("semestralka_output/GRAF_1_avg_prices_products.html", index = False)
	prcen_low = prcen.filter(col("avgprice") < 5000).toPandas()
	prcen_high = prcen.filter(col("avgprice") >= 5000).toPandas()
	plt.close()
	plt.hist(prcen_low["avgprice"],50)
	plt.title('Rozdeleni cen produktu s cenou pod 5000')
	plt.xlabel('Cena')
	plt.ylabel('Cetnost')
	plt.savefig("semestralka_output/GRAF_1_rozdeleni_cen_produktu_pod_5000.png")
	plt.close()
	plt.hist(prcen_high["avgprice"],50)
	plt.title('Rozdeleni cen produktu s cenou nad 5000')
	plt.xlabel('Cena')
	plt.ylabel('Cetnost')
	plt.savefig("semestralka_output/GRAF_1_rozdeleni_cen_produktu_nad_5000.png")
	plt.close()

generate_price_distributions(sales, items)


#ROZDELENI PRUMERNYCH CEN PRODUKTU V RAMCI KATEGORII - GRAF 2
def create_avg_prices_within_cats(sales,spark):
	best_cats = sales.groupBy("itemcat_id").agg(sum("item_cnt_day")).toDF("itemcat","cat_count_total").sort("cat_count_total", ascending = False).limit(5)
	best_cat_sales = sales.join(best_cats,sales.itemcat_id == best_cats.itemcat, how = "left_outer").dropna(subset=('cat_count_total')).select("item_id","itemcat_id","item_price")
	prcencat = best_cat_sales.groupBy("item_id","itemcat_id").agg({"item_price":"avg"}).toDF("item_id","itemcat_id","avgprice").toPandas()
	cats = spark.sql("select * from semestralka_lauderdice.cat")
	bca = prcencat["itemcat_id"].unique()
	for cat in bca:
		plt.hist(prcencat[prcencat.itemcat_id == cat]["avgprice"], 50)
		nvm = cat
		catname = cats.filter(col("cat_id") == int(nvm))
		catname = catname.collect()[0][0]
		plt.title('Rozdeleni cen produktu  - kategorie '+str(catname))
		plt.xlabel('Cena')
		plt.ylabel('Cetnost')
		plt.savefig("semestralka_output/rozdeleni_cen_produktu_pres_kategorie/GRAF_2_kategorie_"+str(cat)+".png")
		plt.close()

create_avg_prices_within_cats(sales,spark)

#ROZDELENI PRUMERNYCH CEN JEN V RAMCI KATEGORII - GRAF 3 + 4
def generate_avg_prices_in_cats(sales,combo, items):
	#best_cats = sales.groupBy("itemcat_id").agg(sum("item_cnt_day")).toDF("itemcat","cat_count_total").sort("cat_count_total", ascending = False).limit(10)
	#best_cat_sales = sales.join(best_cats,sales.itemcat_id == best_cats.itemcat, how = "left_outer").dropna(subset=('cat_count_total')).select("itemcat_id","item_price")
	#prcencat = best_cat_sales.groupBy("itemcat_id").agg({"item_price":"avg"}).toDF("kategorie","Prumerna_cena").sort("Prumerna_cena",ascending = False).toPandas().to_csv("semestralka_output/GRAF_3_avg_prices_categories.csv", index = False, encoding = "UTF-8")
	best_cats = sales.groupBy("itemcat_id").agg({"item_price":"avg"}).toDF("kategorie","prumerna_cena").sort("prumerna_cena",ascending = False).limit(10)
	#to_html("semestralka_output/GRAF_3_avg_prices_categories.html", index = False)
	cats = spark.sql("select * from semestralka_lauderdice.cat")
	best_cats = best_cats.join(cats,cats.cat_id == best_cats.kategorie,"left_outer").select("cat_name","kategorie","prumerna_cena").withColumnRenamed("cat_name","Slovni_nazev_kategie")
	best_cats.toPandas().to_csv("semestralka_output/GRAF_3_avg_prices_categories.csv", index = False, encoding = "UTF-8")
	pricatdist = sales.groupBy("itemcat_id").agg({"item_price":"avg"}).toDF("itemcat_id","avgprice").toPandas()
	plt.hist(pricatdist["avgprice"],50)
	plt.title('Rozdeleni prumernych cen produktu v ramci kategorii')
	plt.xlabel('Cena')
	plt.ylabel('Cetnost')
	plt.savefig("semestralka_output/GRAF_3_rozdeleni_prumernych_cen_produktu_v_ramci_kategorii.png")
	plt.close()
	#
	comboabc = combo.withColumnRenamed("item_id","item")
	prodrev = sales.groupBy("item_id").agg({"revenue":"sum"}).toDF("item_id","revenue")
	abctab = prodrev.join(comboabc, prodrev.item_id == comboabc.item,"left_outer").select("item_id","abc","xyz","revenue")
	items = items.withColumnRenamed("item_id","itemid")
	abctabnames = abctab.join(items,items.itemid ==  abctab.item_id,"left_outer").select("item_name","item_id","abc","xyz","revenue")
	ax = abctabnames.filter((col("abc") == "A") & (col("xyz") == "X")).sort("revenue",ascending = False).limit(10).withColumn("revenue",round(abctabnames["revenue"],2))
	bx = abctabnames.filter((col("abc") == "B") & (col("xyz") == "X")).sort("revenue",ascending = False).limit(10).withColumn("revenue",round(abctabnames["revenue"],2))
	cz = abctabnames.filter((col("abc") == "C") & (col("xyz") == "Z")).sort("revenue",ascending = False).limit(10).withColumn("revenue",round(abctabnames["revenue"],2))
	ax.toPandas().to_csv("semestralka_output/GRAF_4_AX.csv", index = False, encoding = "utf-8")
	#to_html("semestralka_output/GRAF_4_AX.html", index = False)
	bx.toPandas().to_csv("semestralka_output/GRAF_4_BX.csv", index = False, encoding = "utf-8")
	#to_html("semestralka_output/GRAF_4_BX.html", index = False)
	cz.toPandas().to_csv("semestralka_output/GRAF_4_CZ.csv", index = False, encoding = "utf-8")
	#to_html("semestralka_output/GRAF_4_CZ.html", index = False)

generate_avg_prices_in_cats(sales,combo, items)
#Graf časového vývoje celkové tržby po dnech, po týdnech, po měsících
def generate_timeseries(sales):
	time = sales.groupBy("datum").agg({"revenue":"sum"}).toDF("datum","revenue")
	timeday = time.toPandas()
	timeday['datum'] = pd.to_datetime(timeday['datum'])
	timeday = timeday.sort_values(by="datum")
	plt.close()
	f = plt.figure()
	f.set_figwidth(20)
	f.set_figheight(5)
	plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
	plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=20))
	plt.xlabel('Den')
	plt.ylabel('Prodeje')
	plt.plot(timeday['datum'], timeday['revenue'])
	plt.gcf().autofmt_xdate()
	plt.savefig("semestralka_output/GRAF_5_timeline_dny.png")
	plt.close()
	#
	timeday['myear'] = timeday["datum"].dt.strftime('%Y-%m')
	timemonth = timeday.groupby("myear",as_index = False).agg({"revenue":"sum"})
	f = plt.figure()
	f.set_figwidth(20)
	f.set_figheight(5)
	plt.xticks(rotation=45)
	plt.xlabel('Mesic')
	plt.ylabel('Prodeje')
	plt.bar(timemonth['myear'], timemonth['revenue'])
	plt.savefig("semestralka_output/GRAF_5_timeline_mesice.png")
	plt.close()
	#
	timeday['year'] = timeday["datum"].dt.strftime('%Y')
	timeyear = timeday.groupby("year",as_index = False).agg({"revenue":"sum"})
	#f = plt.figure()
	#f.set_figwidth(10)
	#f.set_figheight(5)
	plt.xlabel('Rok')
	plt.ylabel('Prodeje')
	plt.bar(timeyear['year'], timeyear['revenue'])
	plt.savefig("semestralka_output/GRAF_5_timeline_roky.png")
	plt.close()

generate_timeseries(sales)


#DISTRIBUCE PRODEJU VE DNECH MESICICH ATD
def generate_revenue_time_distribution(sales,chart_num, category_id,cat_name = None):
	time = sales.groupBy("datum").agg({"revenue":"sum"}).toDF("datum","revenue")
	timeday = time.toPandas()
	timeday['datum'] = pd.to_datetime(timeday['datum'])
	timeday = timeday.sort_values(by="datum")
	timeday["day"] = timeday.datum.dt.day
	daydistr = timeday[["day","revenue"]].groupby("day", as_index = False).agg({"revenue":"sum"})
	f = plt.figure()
	f.set_figwidth(20)
	f.set_figheight(5)
	plt.xlabel('Den')
	if cat_name != None:
		plt.title('Distribuce prodeju  - kategorie '+str(cat_name))
	plt.xticks(daydistr["day"])
	plt.ylabel('Prodeje')
	plt.bar(daydistr['day'], daydistr['revenue'])
	plt.savefig("semestralka_output/GRAF_"+chart_num+"_timedistr_dnymesice"+category_id+".png")
	plt.close()
	#
	timeday["weekday"] = timeday.datum.dt.weekday + 1
	weekdaydistr = timeday[["weekday","revenue"]].groupby("weekday", as_index = False).agg({"revenue":"sum"})
	f = plt.figure()
	f.set_figwidth(20)
	f.set_figheight(5)
	if cat_name != None:
		plt.title('Distribuce prodeju  - kategorie '+str(cat_name))
	plt.xlabel('Den v tydnu')
	plt.xticks(weekdaydistr["weekday"])
	plt.ylabel('Prodeje')
	plt.bar(weekdaydistr['weekday'], weekdaydistr['revenue'])
	plt.savefig("semestralka_output/GRAF_"+chart_num+"_timedistr_dnytydnu"+category_id+".png")
	plt.close()
	#
	timeday["weeknum"] = timeday.datum.dt.week
	weeknumdistr = timeday[["weeknum","revenue"]].groupby("weeknum", as_index = False).agg({"revenue":"sum"})
	f = plt.figure()
	f.set_figwidth(20)
	f.set_figheight(5)
	plt.xlabel('Tyden roku')
	if cat_name != None:
		plt.title('Distribuce prodeju  - kategorie '+str(cat_name))
	plt.xticks(weeknumdistr["weeknum"])
	plt.ylabel('Prodeje')
	plt.bar(weeknumdistr['weeknum'], weeknumdistr['revenue'])
	plt.savefig("semestralka_output/GRAF_"+chart_num+"_timedistr_tydnyroku"+category_id+".png")
	plt.close()
	#
	timeday["month"] = timeday.datum.dt.month
	monthdistr = timeday[["month","revenue"]].groupby("month", as_index = False).agg({"revenue":"sum"})
	f = plt.figure()
	f.set_figwidth(20)
	f.set_figheight(5)
	plt.xlabel('Mesic roku')
	if cat_name != None:
		plt.title('Distribuce prodeju  - kategorie '+str(cat_name))
	plt.xticks(monthdistr["month"])
	plt.ylabel('Prodeje')
	plt.bar(monthdistr['month'], monthdistr['revenue'])
	plt.savefig("semestralka_output/GRAF_"+chart_num+"_timedistr_mesiceroku"+category_id+".png")
	plt.close()


generate_revenue_time_distribution(sales,"6", "")

#DISTRIBUCE PRODEJU VE DNECH MESICICH ATD dle kategorii
#

def generate_revenue_time_distribution_based_on_cat(sales,spark):
	best_cats = sales.groupBy("itemcat_id").agg(sum("item_cnt_day")).toDF("itemcat","cat_count_total").sort("cat_count_total", ascending = False).limit(5)
	best_cat_sales = sales.join(best_cats,sales.itemcat_id == best_cats.itemcat, how = "left_outer").dropna(subset=('cat_count_total')).select("item_id","itemcat_id","item_price")
	prcencat = best_cat_sales.groupBy("item_id","itemcat_id").agg({"item_price":"avg"}).toDF("item_id","itemcat_id","avgprice").toPandas()
	cats = spark.sql("select * from semestralka_lauderdice.cat")
	bca = prcencat["itemcat_id"].unique()
	for cat in bca:
		catname = cats.filter(col("cat_id") == int(cat))
		catname = catname.collect()[0][0]
		generate_revenue_time_distribution(sales.filter(col("itemcat_id")==int(cat)),"7", "_"+str(cat),catname)


generate_revenue_time_distribution_based_on_cat(sales,spark)
