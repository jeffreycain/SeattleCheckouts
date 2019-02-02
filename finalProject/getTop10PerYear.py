#Find the 100 items with the most checkouts in a month

import pyspark;
sc = pyspark.SparkContext();

kRows = sc.textFile('gs://fisher-gc-bucket/TermProject/seattle-checkouts-by-title/checkouts-by-title.tsv').map(lambda x: (x.split("\t")[6], x.split("\t")))

Rows05 = kRows.filter(lambda x: "Checkouts" not in x[0] and "BOOK" in x[1][3] and "2005" in x[1][4]).map(lambda x: ((int)(x[0]), x[1])).sortByKey(False)
print("---------Done 2005-----------")
Rows06 = kRows.filter(lambda x: "Checkouts" not in x[0] and "BOOK" in x[1][3] and "2006" in x[1][4]).map(lambda x: ((int)(x[0]), x[1])).sortByKey(False)
print("---------Done 2006-----------")
Rows07 = kRows.filter(lambda x: "Checkouts" not in x[0] and "BOOK" in x[1][3] and "2007" in x[1][4]).map(lambda x: ((int)(x[0]), x[1])).sortByKey(False)
print("---------Done 2007-----------")
Rows08 = kRows.filter(lambda x: "Checkouts" not in x[0] and "BOOK" in x[1][3] and "2008" in x[1][4]).map(lambda x: ((int)(x[0]), x[1])).sortByKey(False)
print("---------Done 2008-----------")
Rows09 = kRows.filter(lambda x: "Checkouts" not in x[0] and "BOOK" in x[1][3] and "2009" in x[1][4]).map(lambda x: ((int)(x[0]), x[1])).sortByKey(False)
print("---------Done 2009-----------")
Rows10 = kRows.filter(lambda x: "Checkouts" not in x[0] and "BOOK" in x[1][3] and "2010" in x[1][4]).map(lambda x: ((int)(x[0]), x[1])).sortByKey(False)
print("---------Done 2010-----------")
Rows11 = kRows.filter(lambda x: "Checkouts" not in x[0] and "BOOK" in x[1][3] and "2011" in x[1][4]).map(lambda x: ((int)(x[0]), x[1])).sortByKey(False)
print("---------Done 2011-----------")
Rows12 = kRows.filter(lambda x: "Checkouts" not in x[0] and "BOOK" in x[1][3] and "2012" in x[1][4]).map(lambda x: ((int)(x[0]), x[1])).sortByKey(False)
print("---------Done 2012-----------")
Rows13 = kRows.filter(lambda x: "Checkouts" not in x[0] and "BOOK" in x[1][3] and "2013" in x[1][4]).map(lambda x: ((int)(x[0]), x[1])).sortByKey(False)
print("---------Done 2013-----------")
Rows14 = kRows.filter(lambda x: "Checkouts" not in x[0] and "BOOK" in x[1][3] and "2014" in x[1][4]).map(lambda x: ((int)(x[0]), x[1])).sortByKey(False)
print("---------Done 2014-----------")
Rows15 = kRows.filter(lambda x: "Checkouts" not in x[0] and "BOOK" in x[1][3] and "2015" in x[1][4]).map(lambda x: ((int)(x[0]), x[1])).sortByKey(False)
print("---------Done 2015-----------")
Rows16 = kRows.filter(lambda x: "Checkouts" not in x[0] and "BOOK" in x[1][3] and "2016" in x[1][4]).map(lambda x: ((int)(x[0]), x[1])).sortByKey(False)
print("---------Done 2016-----------")
Rows17 = kRows.filter(lambda x: "Checkouts" not in x[0] and "BOOK" in x[1][3] and "2017" in x[1][4]).map(lambda x: ((int)(x[0]), x[1])).sortByKey(False)
print("---------Done 2017-----------")
Rows18 = kRows.filter(lambda x: "Checkouts" not in x[0] and "BOOK" in x[1][3] and "2018" in x[1][4]).map(lambda x: ((int)(x[0]), x[1])).sortByKey(False)
print("---------Done 2018-----------")


print('\n\n### Results 2005 ###\n');
print(Rows05.take(10));

print('\n\n### Results 2006 ###\n');
print(Rows06.take(10));

print('\n\n### Results 2007 ###\n');
print(Rows07.take(10));

print('\n\n### Results 2008 ###\n');
print(Rows08.take(10));

print('\n\n### Results 2009 ###\n');
print(Rows09.take(10));

print('\n\n### Results 2010 ###\n');
print(Rows10.take(10));

print('\n\n### Results 2011 ###\n');
print(Rows11.take(10));

print('\n\n### Results 2012 ###\n');
print(Rows12.take(10));

print('\n\n### Results 2013 ###\n');
print(Rows13.take(10));

print('\n\n### Results 2014 ###\n');
print(Rows14.take(10));

print('\n\n### Results 2015 ###\n');
print(Rows15.take(10));

print('\n\n### Results 2016 ###\n');
print(Rows16.take(10));

print('\n\n### Results 2017 ###\n');
print(Rows17.take(10));

print('\n\n### Results 2018 ###\n');
print(Rows18.take(10));


