import pyspark;
from operator import add;
sc = pyspark.SparkContext();

kRows = sc.textFile('gs://fisher-gc-bucket/TermProject/seattle-checkouts-by-title/checkouts-by-title.tsv').map(lambda x: (x.split("\t")[6], x.split("\t"))).filter(lambda x: "Checkouts" not in x[0] and "AUDIO" not in x[1][3] and "BOOK" in x[1][3] and "2005" not in x[1][4] and "2006" not in x[1][4] and "2007" not in x[1][4])


#Analytics on Jonathan Franzen: Freedom
##### Excluding Freedom
print("---Franzen Analytics---")
FranzenRows = kRows.filter(lambda x: ("Freedom / Jonathan Franzen." not in x[1][7]) and ("Jonathan Franzen" in x[1][8] or "Franzen, Jonathan" in x[1][8]) and ("2009" in x[1][4] or "2010" in x[1][4] or "2011" in x[1][4] or "2012" in x[1][4]))

print("-----Sample Rows-----");
print(FranzenRows.take(50));

monthRows = FranzenRows;
monthRows = monthRows.map(lambda x: (((int)(x[1][4]), (int)(x[1][5])), (int)(x[0])));
monthCount = monthRows.reduceByKey(add).sortByKey();

print("Month Count:");
print(monthCount.collect());

#Analytics on Jonathan Franzen: Freedom
##### Only Freedom
print("---Franzen Analytics---")
FranzenRows = kRows.filter(lambda x: ("Freedom / Jonathan Franzen." in x[1][7]) and ("Jonathan Franzen" in x[1][8] or "Franzen, Jonathan" in x[1][8]) and ("2009" in x[1][4] or "2010" in x[1][4] or "2011" in x[1][4] or "2012" in x[1][4]))

print("-----Sample Rows-----");
print(FranzenRows.take(50));

monthRows = FranzenRows;
monthRows = monthRows.map(lambda x: (((int)(x[1][4]), (int)(x[1][5])), (int)(x[0])));
monthCount = monthRows.reduceByKey(add).sortByKey();

print("Month Count:");
print(monthCount.collect());


#Analytics on Gillian Flynn: Gone Girl
### Excluding Gone Girl
print("---Gillian Flynn Analytics---")
FlynnRows = kRows.filter(lambda x: ("Gone Girl: A Novel" not in x[1][7] and "Gone girl : a novel / Gillian Flynn." not in x[1][7]) and ("Gillian Flynn" in x[1][8] or "Flynn, Gillian" in x[1][8]) and ("2013" in x[1][4] or "2014" in x[1][4] or "2015" in x[1][4] or "2011" in x[1][4] or "2012" in x[1][4]))

print("-----Sample Rows-----");
print(FlynnRows.take(50));

monthRows = FlynnRows;
monthRows = monthRows.map(lambda x: (((int)(x[1][4]), (int)(x[1][5])), (int)(x[0])));
monthCount = monthRows.reduceByKey(add).sortByKey();

print("Month Count:");
print(monthCount.collect());


#Analytics on Gillian Flynn: Gone Girl
#### Only Gone Girl
print("---Gillian Flynn Analytics---")
FlynnRows = kRows.filter(lambda x: ("Gone Girl: A Novel" in x[1][7] or "Gone girl : a novel / Gillian Flynn." in x[1][7]) and ("Gillian Flynn" in x[1][8] or "Flynn, Gillian" in x[1][8]) and ("2013" in x[1][4] or "2014" in x[1][4] or "2015" in x[1][4] or "2011" in x[1][4] or "2012" in x[1][4]))

print("-----Sample Rows-----");
print(FlynnRows.take(50));

monthRows = FlynnRows;
monthRows = monthRows.map(lambda x: (((int)(x[1][4]), (int)(x[1][5])), (int)(x[0])));
monthCount = monthRows.reduceByKey(add).sortByKey();

print("Month Count:");
print(monthCount.collect());

#"Where'd you go, Bernadette : a novel / Maria Semple.", u'Semple, Maria' 2011-2014
#Analytics on Maria Semple: Where'd you go, Bernadette
#####Excluding Where'd you go, Bernadette
print("---Maria Semple Analytics---")
SempleRows = kRows.filter(lambda x: ("Where'd You Go, Bernadette" not in x[1][7] and "Where'd you go Bernadette" not in x[1][7] and "Where'd you go, Bernadette" not in x[1][7]) and ("Maria Semple" in x[1][8] or "Semple, Maria" in x[1][8]) and ("2013" in x[1][4] or "2014" in x[1][4] or "2011" in x[1][4] or "2012" in x[1][4]))

print("-----Sample Rows-----");
print(SempleRows.take(50));

monthRows = SempleRows;
monthRows = monthRows.map(lambda x: (((int)(x[1][4]), (int)(x[1][5])), (int)(x[0])));
monthCount = monthRows.reduceByKey(add).sortByKey();

print("Month Count:");
print(monthCount.collect());


#"Where'd you go, Bernadette : a novel / Maria Semple.", u'Semple, Maria' 2011-2014
#Analytics on Maria Semple: Where'd you go, Bernadette
##### Only Where'd you go Bernadette
print("---Maria Semple Analytics---")
SempleRows = kRows.filter(lambda x: ("Where'd You Go, Bernadette" in x[1][7] or "Where'd you go Bernadette" in x[1][7] or "Where'd you go, Bernadette" in x[1][7]) and ("Maria Semple" in x[1][8] or "Semple, Maria" in x[1][8]) and ("2013" in x[1][4] or "2014" in x[1][4] or "2011" in x[1][4] or "2012" in x[1][4]))

print("-----Sample Rows-----");
print(SempleRows.take(50));

monthRows = SempleRows;
monthRows = monthRows.map(lambda x: (((int)(x[1][4]), (int)(x[1][5])), (int)(x[0])));
monthCount = monthRows.reduceByKey(add).sortByKey();

print("Month Count:");
print(monthCount.collect());


#'All the Light We Cannot See: A Novel', u'Anthony Doerr' 2014-2016
#Analytics on Anthony Doerr: All the Light We Cannot See
#### Excluding All the light we cannot see
print("---Anthony Doerr Analytics---")
DoerrRows = kRows.filter(lambda x: ("All the Light We Cannot See: A Novel" not in x[1][7] and "All the light we cannot see" not in x[1][7]) and ("Anthony Doerr" in x[1][8] or "Doerr, Anthony" in x[1][8]) and ("2015" in x[1][4] or "2014" in x[1][4] or "2016" in x[1][4]))

print("-----Sample Rows-----");
print(DoerrRows.take(50));

monthRows = DoerrRows;
monthRows = monthRows.map(lambda x: (((int)(x[1][4]), (int)(x[1][5])), (int)(x[0])));
monthCount = monthRows.reduceByKey(add).sortByKey();

print("Month Count:");
print(monthCount.collect());

#'All the Light We Cannot See: A Novel', u'Anthony Doerr' 2014-2016
#Analytics on Anthony Doerr: All the Light We Cannot See
##### Only All the light we cannot see
print("---Anthony Doerr Analytics---")
DoerrRows = kRows.filter(lambda x: ("All the Light We Cannot See: A Novel" in x[1][7] or "All the light we cannot see" in x[1][7]) and ("Anthony Doerr" in x[1][8] or "Doerr, Anthony" in x[1][8]) and ("2015" in x[1][4] or "2014" in x[1][4] or "2016" in x[1][4]))

print("-----Sample Rows-----");
print(DoerrRows.take(50));

monthRows = DoerrRows;
monthRows = monthRows.map(lambda x: (((int)(x[1][4]), (int)(x[1][5])), (int)(x[0])));
monthCount = monthRows.reduceByKey(add).sortByKey();

print("Month Count:");
print(monthCount.collect());

##############################################################################################
# This section is useless, not enough other titles / months to even plot data

#'The Martian: A Novel', u'Andy Weir' First appeared Jan 2016
#Analytics on Andy Weir: The Martian
#print("---Andy Weir Analytics---")
#WeirRows = kRows.filter(lambda x: ("The Martian: A Novel" not in x[1][7] and "The Martian" not in x[1][7] and "El marciano" not in x[1][7]) and ("Andy Weir" in x[1][8] or "Weir, Andy" in x[1][8]) and ("2015" in x[1][4] or "2016" in x[1][4] or "2017" in x[1][4]))

#print("-----Sample Rows-----");
#print(WeirRows.take(50));

#monthRows = WeirRows;
#monthRows = monthRows.map(lambda x: (((int)(x[1][4]), (int)(x[1][5])), (int)(x[0])));
#monthCount = monthRows.reduceByKey(add).sortByKey();

#print("Month Count:");
#print(monthCount.collect());
##############################################################################################

#The midnight line / Lee Child.', u'Child, Lee' 2016-2018
#Analytics on Lee Child: The midnight line
#### Excluding the midnight line
print("---Lee Child Analytics---")
ChildRows = kRows.filter(lambda x: ("The midnight line" not in x[1][7]) and ("Lee Child" in x[1][8] or "Child, Lee" in x[1][8]) and ("2016" in x[1][4] or "2017" in x[1][4] or "2018" in x[1][4]))

print("-----Sample Rows-----");
print(ChildRows.take(50));

monthRows = ChildRows;
monthRows = monthRows.map(lambda x: (((int)(x[1][4]), (int)(x[1][5])), (int)(x[0])));
monthCount = monthRows.reduceByKey(add).sortByKey();

print("Month Count:");
print(monthCount.collect());


#The midnight line / Lee Child.', u'Child, Lee' 2016-2018
#Analytics on Lee Child: The midnight line
### Only the midnight line
print("---Lee Child Analytics---")
ChildRows = kRows.filter(lambda x: ("The midnight line" in x[1][7]) and ("Lee Child" in x[1][8] or "Child, Lee" in x[1][8]) and ("2016" in x[1][4] or "2017" in x[1][4] or "2018" in x[1][4]))

print("-----Sample Rows-----");
print(ChildRows.take(50));

monthRows = ChildRows;
monthRows = monthRows.map(lambda x: (((int)(x[1][4]), (int)(x[1][5])), (int)(x[0])));
monthCount = monthRows.reduceByKey(add).sortByKey();

print("Month Count:");
print(monthCount.collect());


#'Commonwealth : a novel / Ann Patchett.', u'Patchett, Ann' 2015-2017
#Analytics on Ann Patchett: Commonwealth
#### Excluding Commonwealth
print("---Ann Patchett Analytics---")
PatchettRows = kRows.filter(lambda x: ("Commonwealth" not in x[1][7]) and ("Ann Patchett" in x[1][8] or "Patchett, Ann" in x[1][8]) and ("2015" in x[1][4] or "2016" in x[1][4] or "2017" in x[1][4]))

print("-----Sample Rows-----");
print(PatchettRows.take(50));

monthRows = PatchettRows;
monthRows = monthRows.map(lambda x: (((int)(x[1][4]), (int)(x[1][5])), (int)(x[0])));
monthCount = monthRows.reduceByKey(add).sortByKey();

print("Month Count:");
print(monthCount.collect());


#'Commonwealth : a novel / Ann Patchett.', u'Patchett, Ann' 2015-2017
#Analytics on Ann Patchett: Commonwealth
#### Only Commonwealth
print("---Ann Patchett Analytics---")
PatchettRows = kRows.filter(lambda x: ("Commonwealth" in x[1][7]) and ("Ann Patchett" in x[1][8] or "Patchett, Ann" in x[1][8]) and ("2015" in x[1][4] or "2016" in x[1][4] or "2017" in x[1][4]))

print("-----Sample Rows-----");
print(PatchettRows.take(50));

monthRows = PatchettRows;
monthRows = monthRows.map(lambda x: (((int)(x[1][4]), (int)(x[1][5])), (int)(x[0])));
monthCount = monthRows.reduceByKey(add).sortByKey();

print("Month Count:");
print(monthCount.collect());


#'The hunger games / Suzanne Collins.', u'Collins, Suzanne'
#'Catching fire / Suzanne Collins.', u'Collins, Suzanne'
#Analytics on Suzanne Collins: The Hunger Games Series
######### Look at the pattern of her books other than Hunger Games
print("---Suzanne Collins Analytics---")
CollinsRows = kRows.filter(lambda x: ("hunger games" not in x[1][7] and "Catching fire" not in x[1][7] and "Mockinjay" not in x[1][7] and "Hunger Games" not in x[1][7] and "Catching Fire" not in x[1][7]) and ("Suzanne Collins" in x[1][8] or "Collins, Suzanne" in x[1][8]) and ("2008" in x[1][4] or "2009" in x[1][4] or "2010" in x[1][4] or "2011" in x[1][4] or "2012" in x[1][4] or "2013" in x[1][4] or "2014" in x[1][4] or "2015" in x[1][4] or "2016" in x[1][4] or "2017" in x[1][4]))

print("-----Sample Rows-----");
print(CollinsRows.take(100));

monthRows = CollinsRows;
monthRows = monthRows.map(lambda x: (((int)(x[1][4]), (int)(x[1][5])), (int)(x[0])));
monthCount = monthRows.reduceByKey(add).sortByKey();

print("Month Count:");
print(monthCount.collect());


#'The hunger games / Suzanne Collins.', u'Collins, Suzanne'
#'Catching fire / Suzanne Collins.', u'Collins, Suzanne'
#Analytics on Suzanne Collins: The Hunger Games Series
########## Look at the pattern of the Hunger Games Series
print("---Suzanne Collins Analytics---")
CollinsRows = kRows.filter(lambda x: ("hunger games" in x[1][7] or "Catching fire" in x[1][7] or "Mockinjay" in x[1][7] or "Hunger Games" in x[1][7] or "Catching Fire" in x[1][7]) and ("Suzanne Collins" in x[1][8] or "Collins, Suzanne" in x[1][8]) and ("2008" in x[1][4] or "2009" in x[1][4] or "2010" in x[1][4] or "2011" in x[1][4] or "2012" in x[1][4] or "2013" in x[1][4] or "2014" in x[1][4] or "2015" in x[1][4] or "2016" in x[1][4] or "2017" in x[1][4]))

print("-----Sample Rows-----");
print(CollinsRows.take(100));

monthRows = CollinsRows;
monthRows = monthRows.map(lambda x: (((int)(x[1][4]), (int)(x[1][5])), (int)(x[0])));
monthCount = monthRows.reduceByKey(add).sortByKey();

print("Month Count:");
print(monthCount.collect());


#Analytics on Stieg Larsson: The Girl who kicked the hornet's nest
### Only the millenium series
print("---Stieg Larsson Analytics---")
LarRows = kRows.filter(lambda x: ("hornet's nest" in x[1][7] or "Hornet's Nest" in x[1][7] or "dragon tattoo" in x[1][7] or "Dragon Tatoo" in x[1][7] or "played with fire" in x[1][7] or "Played with Fire" in x[1][7]) and ("Stieg Larsson" in x[1][8] or "Larsson, Stieg" in x[1][8]) and ("2007" in x[1][4] or "2008" in x[1][4] or "2009" in x[1][4] or "2010" in x[1][4] or "2011" in x[1][4] or "2012" in x[1][4] or "2013" in x[1][4] or "2014" in x[1][4]))

monthRows = LarRows;
monthRows = monthRows.map(lambda x: (((int)(x[1][4]), (int)(x[1][5])), (int)(x[0])));
monthCount = monthRows.reduceByKey(add).sortByKey();

print("-----Sample Rows-----");
print(LarRows.take(50));

print("Month Count:");
print(monthCount.collect());




