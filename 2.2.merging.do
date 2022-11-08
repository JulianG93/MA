*** DO-FILE:       Merging  *************************************************
*** PROJECT NAME:  Rice Insurance..................................**********
*** DATE: 		   13.05.2014 ***********************************************
*** AUTHOR: 	   JZ *******************************************************

set more off
  
** RUN SETTINGS **
clear
// cd "/Users/`c(username)'/Dropbox/Research/DFG_FOR576/3. Research/Rice Insurance/2. STATA/do" // Old directory
cd "//tsclient/C/Users/Julian/Desktop/Thesis/Rice_Long_Term_Bearbeitung/2. STATA/do/_Juliane alt" // Changed cd to my directory
macro drop _all // Alle Makros werden gelöscht, damit potentielle alte noch vorhandene Macros nicht zu Problemen führen
// do 2.1.settings.do Add 09.11.: Not needed as the settings are in the master.do-file                 


*****************************************************************************
*** SECTION 2.2a - 2013 MERGING  ********************************************


** CREATE MASTER **


foreach wave in  w5 w1  w2 w3  {

cd "${cleandata_`wave'}" 
use hh.dta

foreach root in 10024 12122 12123 71133b 71133c 72201 32007 32010 31024 31025 31013a 31013b 31014a 31014b 31019a 31019b 31020a 31020b 32011a 32011b 32011c 32011d 32011e 42030 62013 62001 62022 62024 62027 62020 62021 62023 62025 62026{
cap rename  __`root' _x`root'
}

cap gen prov= __10001
cap gen distr= __10002
cap gen subdistr= __10003
cap gen vill= __10004

drop if T!=1

tostring QID, replace force

gen n60relfr= 0 if _x32011a!=.
foreach var of varlist _x32011a _x32011b _x32011c _x32011d _x32011e {
replace n60relfr=1 if `var'== 7 | `var'== 9 | `var'== 17 | `var'== 18 | `var'== 30 | `var'== 31
}
gen n60borgov= 0 if _x32011a!=.
foreach var of varlist _x32011a _x32011b _x32011c _x32011d _x32011e {
replace n60borgov=1 if `var'== 21 | `var'== 23 | `var'== 24 | `var'== 25 | `var'== 28 
}

recode _x72201 (1=1) (2=0) 

if "`wave'"!="w1"{
foreach var of varlist _x31013a _x31013b _x31014a _x31014b _x31019a _x31019b _x31020a _x31020b {
recode `var' (1 2 =1) (nonmis=0)
}
}

cd "${cleandata_`wave'}"
save hh_temp.dta, replace

if "`wave'"=="w5"{
keep QID hhid prov distr subdistr  vill T   _x12122 _x12123 _x42030 _x32007 _x32010 _x32024 _x31013a _x31013b _x31014a _x31014b _x31019a _x31019b _x31020a _x31020b _x72201 n60borgov n60relfr
}
if "`wave'"=="w2" | "`wave'"=="w3"{ 
keep QID hhid prov distr subdistr  vill T   _x12122 _x12123 _x42030 _x32007 _x32010         _x31013a _x31013b _x31014a _x31014b _x31019a _x31019b _x31020a _x31020b _x72201 n60borgov n60relfr
}
if "`wave'"=="w1"{
keep QID hhid prov distr subdistr  vill T   _x12122 _x12123 _x42030 _x32007 _x32010        _x72201 n60borgov n60relfr
}


cd "$data"
save ${merge_`wave'}, replace

cap merge 1:1 QID using "${cleandata_`wave'}/hh_temp.dta", keepusing(_x62013 _x62001 _x62022 _x62024 _x62027 _x62020 _x62021 _x62023 _x62025 _x62026 ) nogen //not in 2007 and 2008
foreach var in _x62013 _x62001 _x62022 _x62024 _x62027 _x62020 _x62021 _x62023 _x62025 _x62026{
cap replace `var'=0 if `var'!=1 & `var'!=.
}

cap merge 1:1 QID using "${cleandata_`wave'}/hh_temp.dta", keepusing(_x10024 _x31024 _x31025 _x71133b _x71133c _x31013a _x31013b _x31014a _x31014b _x31019a _x31019b _x31020a _x31020b _x72201 n60borgov n60relfr) nogen //not in 2007


cd "$data"
save ${merge_`wave'}, replace

erase "${cleandata_`wave'}/hh_temp.dta"
}
*



*****************************************************************************




** MERGE MEM DATA**

foreach wave in  w2 w3 w5 w1 {

cd "${cleandata_`wave'}"
use mem.dta, clear

//for w1, these are the only jointly defined vars 
foreach root in 21004 21005 21003 21011 21012 21013 21014 21018 21020n 22003 22006 22005 22007 23003{
cap rename  __`root' _x`root'
}
cap rename _x21020n _x21020

//hh age structure and children below 6
gen under6= _x21004<6 if _x21004 < .

gen over14= _x21004>14 if _x21004 < .

gen hhavage= _x21004

gen hhhage= _x21004 if _x21005==1


//agegroups
gen btw020=  (_x21004>=0  & _x21004<20) if _x21004!=.
gen btw2040= (_x21004>=20 & _x21004<40) if _x21004!=.
gen btw4060= (_x21004>=40 & _x21004<60) if _x21004!=.
gen btw60up= (_x21004>=60)              if _x21004!=.

gen btw1016=  (_x21004>=10  & _x21004<=16) if _x21004!=.
gen btw1115= (_x21004>=11 & _x21004<=15) if _x21004!=.
gen btw1214= (_x21004>=12 & _x21004<=14) if _x21004!=.

gen btw1564f= (_x21004>=15 & _x21004<=64) & _x21003==2 if _x21004!=.
gen btw1564m= (_x21004>=15 & _x21004<=64) & _x21003==1 if _x21004!=.

//occupation
gen jobsear = (_x21018==4 | _x21018==5) if _x21018!=.

gen remittjsD = ( _x21020> 0 & jobsear==1) if jobsear!=. & _x21020!=.

tab _x21014, gen(oc)

egen oc_ag  = rowtotal(oc1 oc4 oc6)
egen oc_na = rowtotal(oc3 oc5 oc7 oc8 oc15)
egen oc_ue = rowtotal(oc12)
egen oc_st = rowtotal(oc10)
egen oc_ot = rowtotal(oc2 oc9 oc13 oc14)

egen anymem_oc_go = rowtotal(oc8) 

 
//hh gender structure
gen _x21004_malegrownup=1  if _x21004 >12 & _x21003==1
replace  _x21004_malegrownup=0  if _x21004_malegrownup==.

gen _x21004_grownup= _x21004 >12   if _x21004<.

gen hhhgen= _x21003 if _x21005==1


//eduction of hh
gen edhhh_repair= 0 			if _x22006==2 & _x22007==. & (_x21005==1 | _x21005==2)
replace edhhh_repair=_x22005 	if _x22005!=. & _x22007==. & (_x21005==1 | _x21005==2) 
replace _x22007=edhhh_repair 	if _x22007==.

*gen eduhhh=_x22007 if _x21005==1
*gen edhhhpa=_x22007 if _x21005==2

gen hhhprim=	(_x22007 <5 & _x21005==1) 
gen hhhprim7=	(_x22007 <8 & _x21005==1)
gen hhhsec= 	(_x22007 >=5 & _x21005==1)
replace hhhprim=. if _x21005!=1
replace hhhprim7=. if _x21005!=1
replace hhhsec=. if _x21005!=1

bys hhid: egen hheduhi = max(_x22007) if T==1
bys hhid: gen temp=_n
replace hheduhi=. if temp!=1
label value hheduhi __22007
fre hheduhi if T==1
drop temp

gen hhedupr = (hheduhi<=7) if hheduhi!=. & T==1
gen hheduls = (hheduhi<=10) if hheduhi!=. & T==1

//can read write
recode _x22003 (1=1) (2=0), gen(hhhread)
replace hhhread=. if _x21005!=1

//subjective health assessment
recode _x23003 (1=1) (nonmis=0), gen(hhh_healthy)  
recode _x23003 (2=1) (nonmis=0), gen(hhh_canmanage)
recode _x23003 (3=1) (nonmis=0), gen(hhh_sick)
replace hhh_healthy=. if _x21005!=1
replace hhh_canmanage=. if _x21005!=1
replace hhh_sick=. if _x21005!=1

//hhh missing
gen hhhnomis=1 if _x21005==1

//ethnicity and political membership
gen hhhethn= _x21011 if _x21005==1
replace hhhethn=0 if hhhethn!=. & hhhethn!=3
replace hhhethn=1 if hhhethn==3 

gen hhhrel= _x21012 if _x21005==1

gen hhhpol= _x21013 if _x21013==1


gen anymem_pol= _x21013
recode anymem_pol (2=0) (1=1)

collapse (max) anymem_pol anymem_oc_go (mean) hhhethn hhhrel hhhpol hhhgen hhhage hhavage hheduhi hhedupr hheduls (sum) btw* hhhnomis hhhprim hhhprim7 hhhsec hhhread hhh_healthy hhh_canmanage hhh_sick under6  _x21004_malegrownup _x21004_grownup (sum) over14 jobsear remittjsD oc_*, by(QID)   

gen genratio=_x21004_malegrownup/_x21004_grownup
drop _x21004_malegrownup _x21004_grownup

gen o14_jobs = jobsear/over14
gen o14_oc_ag  =oc_ag /over14
gen o14_oc_na =oc_na/over14
gen o14_oc_ue =oc_ue/over14
gen o14_oc_st =oc_st/over14
gen o14_oc_ot =oc_ot/over14

replace o14_jobs = 1 if o14_jobs>1 & o14_jobs!=.

gen o14_jobsD = (jobsear>0) if jobsear!=.

replace remittjsD = (remittjsD>0) if remittjsD!=.

drop jobsear  over14 oc_*


*recode eduhhh (0=0) (1/7=1) (8/15=2) (16/22=3) (nonm =4), generate(edhhh_c)

replace hhhpol=0 if hhhpol==.

replace hhhgen=0 if hhhgen==2

save mem_temp, replace

cd "$data"
use ${merge_`wave'}, clear
merge 1:1 QID using "${cleandata_`wave'}/mem_temp.dta"
drop if _merge==2 
drop _merge
erase "${cleandata_`wave'}/mem_temp.dta"

label var hhhethn  "Ethnicity of hh head"
label var hhhrel  "Religion of hh head"
label var hhhpol  "Is 1 if hh head has political membership"
label var anymem_pol  "Is 1 if any mem has political membership"
label var anymem_oc_go  "Is 1 if any mem is government official"

label var hhhgen  "Gender of hh head"
label var hhhage  "Age of hh head"
*label var eduhhh  "Highest edu attainment of hh head"
*label var edhhhpa  "Highest edu attainment of hh head partner"
*label var edhhh_c  "Category of highest edu attainment of hh head"
label var hhhprim  "HH has no or primary edu up to 4th grade"
label var hhhprim7  "HH has no or primary edu up to 7th grade"
label var hhhsec   "HH has more than 4th grade edu up to university degree (rare)"
label var hhhread  "HH can read and write"

label var hhedupr  "HH has no member with edu higher than primary 7"
label var hheduls  "HH has no member with edu higher than lower secondary grade 9"

label var genratio  "Ratio of male grownups (<12) on total grownups in HH"
label var under6  "Number of kids in HH below age 6"
label var o14_jobs "Share of over 14 left HH searching or having job" 
label var o14_jobsD "Dummy if at least 1 HH member left perm for job or jobsearch"
label var hhavage  "HH average age"

label var btw020   "Number of HH mem below 20"
label var btw1016   "Number of HH mem between 10 and inkl 16"
label var btw1115   "Number of HH mem between 11 and inkl 15"
label var btw1214   "Number of HH mem between 12 and inkl 14"
label var btw2040  "Number of HH mem between 20 and 40"
label var btw4060  "Number of HH mem between 40 and 60"
label var btw60up  "Number of HH mem between 60 and up"
label var btw1564f  "Number of woking age HH mem female"
label var btw1564m  "Number of woking age HH mem male"


cd "$data"
save ${merge_`wave'}, replace
}
*





*****************************************************************************


** MERGE SHOCKS **

foreach wave in w1 w2 w3 w5 {

cd "${cleandata_`wave'}"
use shocks.dta, clear

 //for w1, these are the only jointly defined vars 
foreach root in 31001 31002 31004 31005 31006{
cap rename  __`root' _x`root'
}
cap gen _x31005a=_x31005
cap gen _x31005b=.
cap gen _x31006a=_x31006

recode _x31004  (1=3) (2=2) (3=1) (4=0)
label define _x31004 0 "No Impact" 1 "Low Impact" 2 "Medium Impact" 3 "High Impact"
label values _x31004 _x31004

recode _x31002 (10/13=1) (16=2) (21=3) , generate(agrishock)
keep if agrishock <= 3

recode _x31002 (10=1) (nonmiss=0) (miss=.) , generate(flood)
recode _x31002 (10=1) (12=1) (16=1) (55=1) (nonmiss=0) (miss=.) , generate(flood_2)

collapse (count) _x31002 (mean) _x31004 (sum) _x31005a _x31005b _x31006a (max) flood flood_2, by(QID)
save shocks_temp, replace

cd "$data"
use ${merge_`wave'}, clear
merge 1:1 QID using "${cleandata_`wave'}/shocks_temp.dta", keepusing(_x31002 _x31004 _x31005a _x31005b _x31006a flood flood_2)
drop if _merge==2 //keep only hh which were existent in master data before merge
drop _merge
erase "${cleandata_`wave'}/shocks_temp.dta"

replace _x31002=0 if _x31002==.
replace _x31004=0 if _x31002==0 & _x31004==.
replace _x31005a=0 if _x31005a==. & _x31002==0
replace _x31005b=0 if _x31005b==. & _x31002==0
replace _x31006a=0 if _x31006a==. & _x31002==0
replace flood=0 if flood==. & _x31002==0
replace flood_2=0 if flood==. & _x31002==0

gen flood_5a1 = _x31005a if flood==1
replace flood_5a1=0 if flood==0
replace flood_5a1 =. if flood==.
gen flood_5b1 = _x31005b if flood==1
replace flood_5b1=0 if flood==0
replace flood_5b1 =. if flood==.
gen flood_6a1 = _x31006a if flood==1
replace flood_6a1=0 if flood==0
replace flood_6a1 =. if flood==.

gen flood_5a2 = _x31005a if flood_2==1
replace flood_5a2=0 if flood_2==0
replace flood_5a2 =. if flood_2==.
gen flood_5b2 = _x31005b if flood_2==1
replace flood_5b2=0 if flood_2==0
replace flood_5b2 =. if flood_2==.
gen flood_6a2 = _x31006a if flood_2==1
replace flood_6a2=0 if flood_2==0
replace flood_6a2 =. if flood_2==.

label var _x31002 "Number of AGRI shocks in reference period"
label var _x31004 "Average severity of shocks, based on all AGRI shocks in HH survey"
label var _x31005a "Income loss, based on all AGRI shocks in HH survey"
label var _x31005b "Extra expenditure, based on all AGRI shocks in HH survey"
label var _x31006a "Assets loss, based on all AGRI shocks in HH survey"
label var flood "Household reported flooding shock"
label var flood_2 "Household reported flooding, rain, storm, ersosion shock"
label var flood_5a1 "Income loss of Household reported flooding shock"
label var flood_5b1 "Addit. expend. of Household reported flooding shock"
label var flood_6a1 "Asset loss of Household reported flooding shock"
label var flood_5a2 "Income loss of Household reported flooding rel. shock"
label var flood_5b2 "Addit. expend. of Household reported flooding rel. shock"
label var flood_6a2 "Asset loss of Household reported flooding rel. shock"


cd "$data"
save ${merge_`wave'}, replace
}
*


** MERGE AGRI RISKS **

foreach wave in w2 w3 w5 {

cd "${cleandata_`wave'}"
use risks.dta, clear

recode _x32003 _x32003a  (1=3) (2=2) (3=1) (4=0)
label define _x32003 0 "No Impact" 1 "Low Impact" 2 "Medium Impact" 3 "High Impact"
label define _x32003a 0 "No Impact" 1 "Low Impact" 2 "Medium Impact" 3 "High Impact"
label values _x32003 _x32003
label values _x32003a _x32003a

replace _x32002=0 if _x32002==2
replace _x32013=0 if _x32013==2

recode _x32001 (10/16=1) (20/23=2) , generate(agririsk)
keep if agririsk <= 1


collapse (sum) _x32002 _x32004 _x32013 (mean)_x32003 _x32003a, by(QID)
save risks_temp, replace

cd "$data"
use ${merge_`wave'}, clear
merge 1:1 QID using "${cleandata_`wave'}/risks_temp.dta", keepusing(_x32002 _x32003 _x32003a _x32004 _x32013)
drop if _merge==2 //keep only hh which were existent in master data before merge
drop _merge
erase "${cleandata_`wave'}/risks_temp.dta"

replace _x32002=0 if _x32002==.
replace _x32003=0 if _x32003==.
replace _x32003a=0 if _x32003a==.
replace _x32004=0 if _x32004==.
replace _x32013=0 if _x32013==.


rename _x32002  _x32002 
rename _x32003  _x32003 
rename _x32003a  _x32003a
rename _x32004  _x32004
rename _x32013  _x32013


label var _x32002 "Number of AGRI risk categories in future 5 years"
label var _x32004 "Number of total AGRI risks in future 5 years"
label var _x32013 "Number of total AGRI risks categories where preventive action undertaken"
label var _x32003 "Average severity of risk on income, based on all AGRI risks"
label var _x32003a "Average severity of risk on assets, based on all AGRI risks"


cd "$data"
save ${merge_`wave'}, replace
}
*



** MERGE WEATHER, REGULATION AND PRICE RISKS **

foreach wave in w5 w2 w3  {

cd "${cleandata_`wave'}"
use risks.dta, clear

replace _x32002=0 if _x32002==2

recode _x32001 (10/12 16 55 =100)  , generate(weatrisk) //100=making up a number that does not already exist in the risk list
recode _x32001 (23=100) , generate(mregrisk) //100=making up a number that does not already exist in the risk list
recode _x32001 (21=100) , generate(outprisk) //100=making up a number that does not already exist in the risk list

gen weatriskD = 1 if _x32002==1 & weatrisk==100
gen mregriskD = 1 if _x32002==1 & mregrisk==100
gen outpriskD = 1 if _x32002==1 & outprisk==100

collapse (sum) weatriskD mregriskD outpriskD, by(QID)
save risks2_temp, replace

cd "$data"
use ${merge_`wave'}, clear
merge 1:1 QID using "${cleandata_`wave'}/risks2_temp.dta", keepusing(weatriskD mregriskD outpriskD)
drop if _merge==2 //keep only hh which were existent in master data before merge
drop _merge
erase "${cleandata_`wave'}/risks2_temp.dta"

replace weatriskD=0 if weatriskD==.
replace weatriskD=1 if weatriskD!=. & weatriskD!=0
replace mregriskD=0 if mregriskD==.
replace outpriskD=0 if outpriskD==.

label var weatriskD "HH anticipates severe weather events risk next 5 years"
label var mregriskD "HH anticipates change market regulations risk, next 5 years"
label var outpriskD "HH anticipates strong decrease output price risk, next 5 years"

cd "$data"
save ${merge_`wave'}, replace
}
*



*****************************************************************************

** MERGE LAND DATA **

foreach wave in     w5 w1 w2 w3{

cd "${cleandata_`wave'}"
use land.dta, clear

cap rename __41005 _x41005
cap rename __41003 _x41003
cap gen _x41009a=.

drop if T!=1 //there was a problem merging w5 land data for some provinces apparently, but buriram seems ok
cap drop __000000 __000001
cap tostring QID, force replace

recode _x41005 (11/12=1) (1/3=2), gen(owned)
*recode _x41005 (11/12=1) (1=2), gen(owned)

gen ownlanda =_x41003 if owned<=2
gen ownlandv =_x41009a if owned<=2

collapse  (sum) _x41003 _x41009a ownlanda ownlandv , by(QID)   

replace _x41003=0 if _x41003==.
replace _x41009a=0 if _x41009a==.

gen ownlandaS = ownlanda/_x41003

save land_temp, replace 

cd "$data"
use ${merge_`wave'}, clear
merge 1:1 QID using "${cleandata_`wave'}/land_temp.dta"
drop if _merge==2 
drop _merge
erase "${cleandata_`wave'}/land_temp.dta"

label var _x41003  "Total land area"
label var _x41009a  "Total land value"
label var ownlanda  "Total owned land area"
label var ownlandv  "Total owned land value"


cd "$data"
save ${merge_`wave'}, replace
}
*

** MERGE MORE LAND DATA **


foreach wave in     w5 w1 w2 w3{

cd "${cleandata_`wave'}"
use land.dta, clear

cap rename __41004 _x41004
cap rename __41003 _x41003
cap gen _x41009a=.

drop if T!=1 //there was a problem merging w5 land data for some provinces apparently, but buriram seems ok
cap drop __000000 __000001
cap tostring QID, force replace

gen ricelanda =_x41003 if _x41004==3
gen ricelandv =_x41009a if _x41004==3

collapse  (sum) ricelanda ricelandv , by(QID)   

gen ricelandp =ricelandv/ricelanda 
winsor2 ricelandp , cut(1 97) replace

save land2_temp, replace 

cd "$data"
use ${merge_`wave'}, clear
merge 1:1 QID using "${cleandata_`wave'}/land2_temp.dta"
drop if _merge==2 
drop _merge
erase "${cleandata_`wave'}/land2_temp.dta"

label var ricelandp  "Rice or field crop land, average price per rai"

cd "$data"
save ${merge_`wave'}, replace
}
*



** Generate Sample Var: MERGE CROPS; LAND DATA **

foreach wave in    w2 w1  w5 w3    {

cd "${cleandata_`wave'}"
use crops.dta, clear

cap drop __000000
*cap tostring QID, force replace
drop if T!=1 //there was a problem merging w5 land data for some provinces apparently, but buriram seems ok


 //for w1, these are the only jointly defined vars 
foreach root in 42002 42004 42005  42006 42008 42009 42010 42014 42016  { 
cap rename  __`root' _x`root'
}

keep   _x42002   _x42004 _x42005 _x42006 _x42008  _x42009 _x42010 _x42014 _x42016  QID 

if "`wave'"=="w1" | "`wave'"=="w2" {
	replace _x42005=_x42005*6.25
}
*

rename _x42004 parcelid 
save crops_temp, replace


use land.dta, clear

cap drop __000000
*cap tostring QID, force replace
drop if T!=1 //there was a problem merging w5 land data for some provinces apparently, but buriram seems ok


 //for w1, these are the only jointly defined vars 
foreach root in 41002 41003 41005 41008  { 
cap rename  __`root' _x`root'
}


cap gen _x41009a=.


if "`wave'"=="w5" {
	rename ID parcelid //w5 problem: parcel IDs are SQLIDs 
}
else {
	rename _x41002 parcelid
}

keep _x41003 _x41005  _x41008 _x41009a parcelid QID
cap tostring QID, replace

drop if QID =="3410020302                    " & _x41003==.
drop if QID =="3415160206                    " & _x41003==.


//merge crop information to the parcels
merge 1:m QID parcelid using "${cleandata_`wave'}/crops_temp.dta" ,nogen
save land_temp, replace

//drop if not rice cassava corn crops
recode _x42002 (101/104=1) , gen(insurcrops)
keep if insurcrops==1 
 
//unit transformation
gen unit=.
replace unit = 1000 if _x42009==1
replace unit = 1 if _x42009==2
replace unit = 12 if _x42009==7
replace unit = 2.2 if _x42009==11

//output in kg per crop
gen ricekgtot   = _x42010*unit 
gen ricekgav = _x42010*unit 
gen riceland = _x42005
gen ricelando= _x42005 if _x41005==1 | _x41005==2 | _x41005==3

collapse (mean) insurcrops  riceland ricelando ricekgav (sum) ricekgtot, by(QID parcelid)   

collapse (mean) insurcrops  (sum) riceland ricelando ricekgav ricekgtot , by(QID)   

replace ricelando =0 if ricelando==.
replace ricekgtot   =0 if ricekgtot==.
replace ricekgav =0 if ricekgav==.

gen ricekgrai=ricekgav / riceland
drop ricekgav

save land_temp, replace

cd "$data"
use ${merge_`wave'}, clear
merge 1:1 QID using "${cleandata_`wave'}/land_temp.dta"
drop if _merge==2 
drop _merge
erase "${cleandata_`wave'}/land_temp.dta"

replace riceland=0 if riceland==.

label var ricekgrai  "Rice produce in kg/rai on aver/plot (sev harvests possbl)"
label var ricekgtot  "Rice produce in in total per year"
label var riceland  "Total area planted with rice"
label var ricelando  "Total area planted with rice and owned"
label var insurcrops "Does HH plant RICE crops"


cd "$data"
save ${merge_`wave'}, replace
}
*

** AGAIN MERGE CROPS DATA FOR TAPIOCA AND MAIZE**

foreach wave in    w2 w1  w5 w3    {

cd "${cleandata_`wave'}"
use crops.dta, clear

cap drop __000000
*cap tostring QID, force replace
drop if T!=1 //there was a problem merging w5 land data for some provinces apparently, but buriram seems ok


 //for w1, these are the only jointly defined vars 
foreach root in 42002 42004 42005  42006 42008 42009 42010 42014 42016  { 
cap rename  __`root' _x`root'
}

keep   _x42002   _x42004 _x42005 _x42006 _x42008  _x42009 _x42010 _x42014 _x42016  QID 

if "`wave'"=="w1" | "`wave'"=="w2" {
	replace _x42005=_x42005*6.25
}
*

rename _x42004 parcelid 
save crops2_temp, replace


use land.dta, clear

cap drop __000000
*cap tostring QID, force replace
drop if T!=1 //there was a problem merging w5 land data for some provinces apparently, but buriram seems ok


 //for w1, these are the only jointly defined vars 
foreach root in 41002 41003 41005 41008  { 
cap rename  __`root' _x`root'
}


cap gen _x41009a=.


if "`wave'"=="w5" {
	rename ID parcelid //w5 problem: parcel IDs are SQLIDs 
}
else {
	rename _x41002 parcelid
}

keep _x41003 _x41005  _x41008 _x41009a parcelid QID
cap tostring QID, replace

drop if QID =="3410020302                    " & _x41003==.
drop if QID =="3415160206                    " & _x41003==.


//merge crop information to the parcels
merge 1:m QID parcelid using "${cleandata_`wave'}/crops2_temp.dta" ,nogen
save land2_temp, replace
erase crops2_temp.dta

//do our rice farmers also grwo corn, cassava
recode _x42002 (6 201/202 =1) (nonmis=0), gen(temp)
bysort QID: egen corncass = max(temp)
drop temp

recode _x42002 (6  =1) (nonmis=0), gen(temp)
bysort QID: egen cass = max(temp)
drop temp

recode _x42002 (201/202 =1) (nonmis=0), gen(temp)
bysort QID: egen corn = max(temp)
drop temp


tab _x42009 if _x42002==6 
tab _x42009 if _x42002==201 | _x42002==202

tab _x42010 if _x42002==6 & _x42009==2
tab _x42010 if _x42002==6 & _x42009==16
tab _x42010 if _x42002==6 & _x42009==17

tab _x42010 if (_x42002==201 | _x42002==202) & _x42009==2 //total 19, and only 6 farmers in the raw sample grow 1 ton or more corn, the rest grows below or equal to 120 kg


//unit transformation
gen unit=.
replace unit = 1000 if _x42009==1
replace unit = 1 if _x42009==2
replace unit = 12 if _x42009==7
replace unit = 2.2 if _x42009==11
replace unit = 5000 if _x42009==17
replace unit = 2 if _x42009==16

//output cassva corn in kg per crop
gen temp   = _x42010*unit if  _x42002==6
bysort QID: egen casskgtot = total(temp)
drop temp
gen temp   = _x42010*unit if  _x42002==201 | _x42002==202
bysort QID: egen cornkgtot = total(temp)
drop temp


collapse (mean) corncass corn cass casskgtot cornkgtot , by(QID parcelid)   

collapse (mean) corncass corn cass casskgtot cornkgtot , by(QID)   

replace casskgtot   =0 if casskgtot==.
replace cornkgtot   =0 if cornkgtot==.

save land2_temp, replace

cd "$data"
use ${merge_`wave'}, clear
merge 1:1 QID using "${cleandata_`wave'}/land2_temp.dta"
drop if _merge==2 
drop _merge
erase "${cleandata_`wave'}/land2_temp.dta"

label var corncass "Does HH plant corn, cassava"
label var corn "Does HH plant corn"
label var cass "Does HH plant cassava"
label var casskgtot  "Cassava produce in in total per year"
label var cornkgtot  "Corn produce in in total per year"



cd "$data"
save ${merge_`wave'}, replace
}
*



** MERGE MORE CROP DATA **

foreach wave in   w2 w1   w3 w5  {

cd "${cleandata_`wave'}"
use crops.dta, clear


 //for w1, these are the only jointly defined vars 
foreach root in 42002 42009 42010 42013a 42014 42014b 42016 42016b 42018 42019 42020 42021 42022 42023 42024 42025 42025a 42025b 42025c 42026 42027 42028 42029 42029a 42036  { 
cap rename  __`root' _x`root' 
}


/*
if "`wave'"=="w1" | "`wave'"=="w2" {
	replace _x42005=_x42005*6.25
}
*/

cap gen _x42013a=.
cap gen _x42029a =.
cap gen _x42036 =.
cap gen _x42014b =.
cap gen _x42016b=.

//equalize
cap gen _x42025= _x42025a + _x42025b + _x42025c
cap drop _x42025a _x42025b  _x42025c

//reduce vars
keep _x42002 _x42009 _x42010 _x42013a  _x42014 _x42014b _x42016 _x42016b _x42019 _x42020 _x42021 _x42022 _x42023 _x42024 _x42025 _x42026 _x42027 _x42028 _x42029 _x42029a _x42036 QID

//drop if not rice crops
recode _x42002 (101/104=1) , gen(insurcrops)
keep if insurcrops==1

//expenditure
gen expend= _x42019 + _x42020 + _x42021 + _x42022 + _x42023 + _x42024 + _x42025 + _x42026 + _x42027 + _x42028 + _x42029 + _x42036
replace expend=_x42029a if expend==.

gen hiredlab= _x42019  + _x42021 + _x42022  + _x42024  + _x42026  + _x42028 


//units
gen unit=.
replace unit = 1000 if _x42009==1
replace unit = 1 if _x42009==2
replace unit = 12 if _x42009==7
replace unit = 2.2 if _x42009==11

//seeds reserved
gen seedsres= _x42013a*unit 


//share sold
egen ricekgsld = rowtotal(_x42014 _x42014b)
replace ricekgsld= ricekgsld*unit 
replace ricekgsld=. if  _x42014 ==.  & _x42014b==.
//winsorize
winsor2 _x42014 , replace 


replace _x42016b=0 if _x42016b==.

gen ricerev= _x42014*_x42016 + _x42014b*_x42016b
sum  _x42014 _x42016 _x42014b _x42016b
sum ricerev, d

gen ricekg= _x42010*unit 

gen pricekgs = ricerev/ricekgsld

if "`wave'"=="w1" {
gen pricekgsD = .
}
if "`wave'"=="w2" {
gen pricekgsD = (pricekgs>=0.7) if pricekgs!=.  //pledging price 14000 / conversion rate 15.93 / 1000
}
if "`wave'"=="w3" {
gen pricekgsD = (pricekgs>=0.7) if pricekgs!=.  //pledging price 14000 * conversion rate 0.0552 / 1000
}
if "`wave'"=="w5" {
gen pricekgsD = (pricekgs>=0.7 ) if pricekgs!=.  //pledging price 15000 * conversion rate 0.0496 / 1000
}

collapse  (sum) expend hiredlab  seedsres ricekgsld ricekg ricerev (max) pricekgsD , by(QID)   

replace seedsres=0 if seedsres==.
replace expend=0 if expend==.
replace hiredlab=0 if hiredlab==.
replace ricekgsld=0 if ricekgsld==.
replace ricekg=0 if ricekg==.
replace ricerev=0 if ricerev==.


gen sharesold= ricekgsld/ricekg  
drop ricekg 

gen pricekg = ricerev/ricekgsld
sum pricekg, d


if "`wave'"=="w1" {
gen pricekgD = .
}
if "`wave'"=="w2" {
gen pricekgD = (pricekg>=0.7) if pricekg!=.  //pledging price 14000 / conversion rate 15.93 / 1000
}
if "`wave'"=="w3" {
gen pricekgD = (pricekg>=0.7) if pricekg!=.  //pledging price 14000 * conversion rate 0.0552 / 1000
}
if "`wave'"=="w5" {
gen pricekgD = (pricekg>=0.7 ) if pricekg!=.  //pledging price 15000 * conversion rate 0.0496 / 1000
}


label var hiredlab  "Rice crop cultivation hired labour expenditure"
label var expend  "Rice crop cultivation expenditure"
label var seedsres  "Rice crop seeds resrved"
label var sharesold  "Share of produced rice that was sold"
label var ricekgsld  "Kg produced rice that was sold"
label var pricekg  "Average price for kg sold rice, all"
label var pricekgD  "Pledging - Avergae price is higher 14000 or 15000 per ton"
label var pricekgsD  "Pledging - Any seperate price is higher 14000 or 15000 per ton"

save crops2_temp, replace

cd "$data"
use ${merge_`wave'}, clear
merge 1:1 QID using "${cleandata_`wave'}/crops2_temp.dta"
drop if _merge==2 
drop _merge
erase "${cleandata_`wave'}/crops2_temp.dta"

cd "$data"
save ${merge_`wave'}, replace

}
*



***#######################THIS MIGHT BE FLAWED################################################
** MERGE MORE CROP DATA **

foreach wave in   w2 w1   w3 w5  {

cd "${cleandata_`wave'}"
use crops.dta, clear


 //for w1, these are the only jointly defined vars 
foreach root in 42002 42009 42010 42013a 42014 42014b 42016 42016b 42018 42019 42020 42021 42022 42023 42024 42025 42025a 42025b 42025c 42026 42027 42028 42029 42029a 42036  { 
cap rename  __`root' _x`root' 
}


/*
if "`wave'"=="w1" | "`wave'"=="w2" {
	replace _x42005=_x42005*6.25
}
*/


if "`wave'"=="w1" {
	gen _x42015a=.
}

cap gen _x42013a=.
cap gen _x42029a =.
cap gen _x42036 =.
cap gen _x42014b =.
cap gen _x42016b=.

//equalize
cap gen _x42025= _x42025a + _x42025b + _x42025c
cap drop _x42025a _x42025b  _x42025c

//reduce vars
keep T _x42002 _x42009 _x42010 _x42013a  _x42014 _x42014b _x42015a _x42016 _x42016b _x42018 _x42019 _x42020 _x42021 _x42022 _x42023 _x42024 _x42025 _x42026 _x42027 _x42028 _x42029 _x42029a _x42036 QID

//drop if not rice crops
recode _x42002 (101/104=1) , gen(insurcrops)
keep if insurcrops==1

gen e_lndprep= _x42018 + _x42019
gen e_seeds= _x42020 + _x42021
gen e_fertil= _x42023 + _x42024
gen e_pestiz= _x42025 + _x42026
gen e_harvest= _x42027 + _x42028
gen e_irrig= _x42029

//units
gen unit=.
replace unit = 1000 if _x42009==1
replace unit = 1 if _x42009==2
replace unit = 12 if _x42009==7
replace unit = 2.2 if _x42009==11

//winsorize
winsor2 _x42014 , replace 

//share sold
egen paddkgsld = rowtotal(_x42014 _x42014b) if _x42015a==1
replace paddkgsld= paddkgsld*unit 
replace paddkgsld=. if  _x42014 ==.  & _x42014b==.

replace _x42016b=0 if _x42016b==.

gen paddrev= _x42014*_x42016 + _x42014b*_x42016b if _x42015a==1
sum  _x42014 _x42016 _x42014b _x42016b
sum paddrev, d

gen ppricekgs = paddrev/paddkgsld

if "`wave'"=="w1" {
gen pprickgsD = .
}
if "`wave'"=="w2" {
gen pprickgsD = (ppricekgs>=0.7) if ppricekgs!=.  //pledging price 14000 / conversion rate 15.93 / 1000
}
if "`wave'"=="w3" {
gen pprickgsD = (ppricekgs>=0.7) if ppricekgs!=.  //pledging price 14000 * conversion rate 0.0552 / 1000
}
if "`wave'"=="w5" {
gen pprickgsD = (ppricekgs>=0.7 ) if ppricekgs!=.  //pledging price 15000 * conversion rate 0.0496 / 1000
}

collapse  (sum) paddrev paddkgsld _x42018 _x42019 _x4202* _x42036 e_* (max) pprickgsD, by(QID)   

drop _x42029a

foreach var of varlist e_* {
replace `var'=0 if `var'==.
winsor2 `var' , cut(0 97) replace
label var `var' "Rice crop cultivation partly expenditures "
}

gen ppricekg = paddrev/paddkgsld
sum ppricekg, d

if "`wave'"=="w1" {
gen ppricekgD = .
}
if "`wave'"=="w2" {
gen ppricekgD = (ppricekg>=0.7) if ppricekg!=.  //pledging price 14000 / conversion rate 15.93 / 1000
}
if "`wave'"=="w3" {
gen ppricekgD = (ppricekg>=0.7) if ppricekg!=.  //pledging price 14000 * conversion rate 0.0552 / 1000
}
if "`wave'"=="w5" {
gen ppricekgD = (ppricekg>=0.7 ) if ppricekg!=.  //pledging price 15000 * conversion rate 0.0496 / 1000
}

label var ppricekg  "Average price for kg sold paddy rice"
label var ppricekgD  "Pledging - Avergae price is higher 14000 or 15000 per ton"
label var pprickgsD  "Pledging - Any seperate price is higher 14000 or 15000 per ton"

save crops3_temp, replace

cd "$data"
use ${merge_`wave'}, clear
merge 1:1 QID using "${cleandata_`wave'}/crops3_temp.dta"
drop if _merge==2 
drop _merge
erase "${cleandata_`wave'}/crops3_temp.dta"

cd "$data"
save ${merge_`wave'}, replace

}
*
***#######################THIS MIGHT BE FLAWED################################################




** MERGE TOTAL LAND PLANTED FROM CROP DATA **

foreach wave in w1 w2 w3 w5 {

cd "${cleandata_`wave'}"
use crops.dta, clear


 //for w1, these are the only jointly defined vars 
foreach root in 42005 42004 { 
cap rename  __`root' _x`root'
}

if "`wave'"=="w1" | "`wave'"=="w2" {
	replace _x42005=_x42005*6.25
}

rename _x42004 parcelid 
rename _x42005 plantland


//reduce vars
keep plantland parcelid QID


collapse (mean) plantland , by(QID parcelid)   

collapse (sum)  plantland , by(QID)   

replace plantland=0 if plantland==.


save crops_temp, replace

cd "$data"
use ${merge_`wave'}, clear
merge 1:1 QID using "${cleandata_`wave'}/crops_temp.dta"
drop if _merge==2 
drop _merge
erase "${cleandata_`wave'}/crops_temp.dta"


gen ricelandS = riceland/plantland
gen otherland= plantland- riceland

label var plantland  "Total area planted"
label var ricelandS  "Share of rice land of total area planted"
label var otherland  "Total area planted that is not planted with rice"

cd "$data"
save ${merge_`wave'}, replace
}
*



*****************************************************************************

** MERGE CROP STORAGE DATA **

foreach wave in w1 w2 w3 w5 {

cd "${cleandata_`wave'}"
use stor.dta, clear

cap rename  __42032  _x42032 

collapse  (sum) _x42032 , by(QID)   
save stor_temp, replace

cd "$data"
use ${merge_`wave'}, clear
merge 1:1 QID using "${cleandata_`wave'}/stor_temp.dta"
drop if _merge==2 
drop _merge
erase "${cleandata_`wave'}/stor_temp.dta"

replace _x42032=0 if _x42032==.

label var _x42032  "Stored total crops in kg  (all not just insured crops)"

cd "$data"
save ${merge_`wave'}, replace
}
*



** MERGE LIVE STOCK DATA **

foreach wave in w1 w2 w3 w5 {

cd "${cleandata_`wave'}"
use livest.dta, clear

//only for w1
cap rename __43109 _x43109a
cap gen _x43103a=.

//lifestock indicator
gen _x43100_D=1

collapse (mean) _x43100_D (sum) _x43103a _x43109a , by(QID)   
save livest_temp, replace

cd "$data"
use ${merge_`wave'}, clear
merge 1:1 QID using "${cleandata_`wave'}/livest_temp.dta"
drop if _merge==2 
erase "${cleandata_`wave'}/livest_temp.dta"

replace _x43103a=0 if _merge==1
replace _x43109a=0 if _merge==1
replace _x43100_D=0 if _merge==1

*gen _x43109_3= _x43109a - _x43103a

label var _x43103a  "Total value of livestock, beginning of period"
label var _x43109a  "Total value of livestock, end of period"
label var _x43100_D "HH has lifestock"

*label var _x43109_3  "Change in value of livestock, over period (positive=increase)"

drop _merge
cd "$data"
save ${merge_`wave'}, replace
}
*


** MERGE LIVESTOCK PRODUCTS  DATA **

foreach wave in w1 w2 w3 w5 {

cd "${cleandata_`wave'}"
use lstprod.dta, clear

//livest prod indicator
gen _x43200_D=1

//no livest prod activities
gen _x43202n =1

collapse  (mean) _x43200_D (sum) _x43202n, by(QID)   
save lstprod_temp, replace

cd "$data"
use ${merge_`wave'}, clear
merge 1:1 QID using "${cleandata_`wave'}/lstprod_temp.dta"
drop if _merge==2 
drop _merge
erase "${cleandata_`wave'}/lstprod_temp.dta"

replace _x43200_D=0 if _x43200_D==.
replace _x43202n=0 if _x43202n==.

label var _x43200_D  "HH has livestock products "
label var _x43202n  "Number of livestock product activities"

cd "$data"
save ${merge_`wave'}, replace
}
*


** MERGE HUNTING/COLLECTING/LOGGING  DATA **

foreach wave in w1 w2 w3 w5 {

cd "${cleandata_`wave'}"
use hunting.dta, clear

//hunting indicator
gen _x44000_D=1

//no of hunting activities
gen _x44002n =1

collapse  (mean) _x44000_D (sum) _x44002n, by(QID)   
save hunting_temp, replace

cd "$data"
use ${merge_`wave'}, clear
merge 1:1 QID using "${cleandata_`wave'}/hunting_temp.dta"
drop if _merge==2 
drop _merge
erase "${cleandata_`wave'}/hunting_temp.dta"

replace _x44000_D=0 if _x44000_D==.
replace _x44002n=0 if _x44002n==.

label var _x44000_D  "HH was Fishing/hunting/collecting/logging "
label var _x44002n  "Number of fishing/hunting/collecting/logging activities"

cd "$data"
save ${merge_`wave'}, replace
}
*


** MERGE OFF-FARM EMPLOYMENT  DATA **

foreach wave in   w3 w5 w1 w2  {
//
cd "${cleandata_`wave'}"
use offempl.dta, clear

//offfarm indicator
gen _x50000_D=1

//no of off farm activities
gen _x50002n =1

//total hours
if "`wave'"=="w1"{
gen _x50028hrsm=. 
rename __50003 _x50003
}
else{
gen _x50028hrsm = (_x50028 * _x50028a * _x50029)/12
}

//income
if "`wave'"=="w1"{
gen _x10087new=. 
}
else{
gen _x10087new = _x50022 if _x50023 ==5 | _x50023 ==6
replace _x10087new = _x50022 * _x50029 if _x50023 ==4
replace _x10087new = _x50022 * _x50029 * _x50028a if _x50023 ==2
winsor2 _x50027c, c(0 99) replace 
replace _x10087new = _x10087new + (_x50027b * _x50029 * _x50028a) + _x50027c if _x50027c!=.
*gen temp = (_x50011a * _x50029 * _x50028a) if _x50008a ==1
*winsor2 temp , c(0 97) replace
*replace _x10087new = _x10087new - temp if temp!=.
*drop temp
}
*

collapse  (mean) _x50000_D (sum) _x50002n _x50028hrsm _x10087new, by(QID _x50003)   

collapse  (mean) _x50000_D (sum) _x50002n _x50028hrsm _x10087new (count) _x50003, by(QID)  
 
save offempl_temp, replace

cd "$data"
use ${merge_`wave'}, clear
merge 1:1 QID using "${cleandata_`wave'}/offempl_temp.dta"
drop if _merge==2 
drop _merge
erase "${cleandata_`wave'}/offempl_temp.dta"

replace _x50000_D=0 if _x50000_D==.
replace _x50003=0 if _x50003==.
replace _x50002n=0 if _x50002n==.
replace _x50028hrsm=0 if _x50028hrsm==.
replace _x10087new=0 if _x10087new==.

label var _x50000_D  "HH has off-farm employment"
label var _x50003  "No of HH mem in off-farm employment"
label var _x50002n  "No of off-farm employment activities"
label var _x50028hrsm  "Total hours HH spend on off-farm employment activities"
label var _x10087new  "Total off-farm income"

if "`wave'"=="w1"{
drop _x50028hrsm  _x10087new
}

cd "$data"
save ${merge_`wave'}, replace
}
*



** MERGE SELF-EMPLOYMENT  DATA **

foreach wave in  w2 w3 w5 w1 {

cd "${cleandata_`wave'}"
use selfempl.dta, clear

//self-empl indicator
gen _x60000_D=1

//no of  self-empl activities
gen _x60002n =1

//total hours
if "`wave'"=="w1"{
gen _x60040hrsm=. 
rename __60003 _x60003
}
else{
gen _x60040hrsm = (_x60040b * _x60040a * _x60039)/12
}

//income
if "`wave'"=="w1"{
gen _x10088new=. 
}
else{
gen _x10088new =_x60038 * _x60039 
}
*


collapse  (mean) _x60000_D (sum)  _x60002n _x60040hrsm _x10088new, by(QID _x60003)   

collapse  (mean) _x60000_D (sum)  _x60002n _x60040hrsm _x10088new (count)  _x60003 , by(QID)  

save selfempl_temp, replace

cd "$data"
use ${merge_`wave'}, clear
merge 1:1 QID using "${cleandata_`wave'}/selfempl_temp.dta"
drop if _merge==2 
drop _merge
erase "${cleandata_`wave'}/selfempl_temp.dta"

replace _x60000_D=0 if _x60000_D==.
replace _x60003=0 if _x60003==.
replace _x60002n=0 if _x60002n==.
replace _x60040hrsm=0 if _x60040hrsm==.
replace _x10088new=0 if _x10088new==.

label var _x60000_D  "HH has self-employment"
label var _x60003  "No of HH mem in slef-farm employment"
label var _x60002n  "No of self-farm employment activities"
label var _x60040hrsm  "Total hours HH spend on self-farm employment activities"
label var _x10088new  "Total self-employ income"

if "`wave'"=="w1"{
drop _x60040hrsm _x10088new
}

cd "$data"
save ${merge_`wave'}, replace
}
*






*****************************************************************************


** MERGE ASSETS DATA **

foreach wave in w2 w3 w5 {

cd "${cleandata_`wave'}"
use assets.dta
gen _x91009_p = _x91009 if _x91008a <3
gen _x91009_a = _x91009 if _x91001 <=11 &_x91001!=5

gen _x91009_1 = _x91002 if _x91001==1
gen _x91009_2 = _x91002 if _x91001==2
gen _x91009_3 = _x91002 if _x91001==3
gen _x91009_4 = _x91002 if _x91001==4
gen _x91009_6 = _x91002 if _x91001==6
gen _x91009_7 = _x91002 if _x91001==7
gen _x91009_8 = _x91002 if _x91001==8
gen _x91009_9 = _x91002 if _x91001==9
gen _x91009_10 = _x91002 if _x91001==10
gen _x91009_11 = _x91002 if _x91001==11


gen tv=( _x91001==26)

collapse (max) tv (sum)  _x91009*  , by(QID prov)




gen _x91009aS = _x91009_a/_x91009

drop prov

save asset_temp, replace

cd "$data"
use ${merge_`wave'}, clear
merge 1:1 QID using "${cleandata_`wave'}/asset_temp.dta"
drop if _merge==2 
drop _merge
erase "${cleandata_`wave'}/asset_temp.dta"


label var _x91009_1  "Number of Traktors 2 wheel"
label var _x91009_2  "Number of Traktors 4 wheel"
label var _x91009_3  "Number of Knapsackspray"
label var _x91009_4  "Number of Engine Spray"
label var _x91009_6  "Number of Water pump"
label var _x91009_7  "Number of Water tanks (field)"
label var _x91009_8  "Number of Pipes"
label var _x91009_9  "Number of Other farm tools"
label var _x91009_10 "Number of Rice Mill"
label var _x91009_11 "Number of Threshing Machine"

label var _x91009 "Total value of HH assets"
label var _x91009_p "Total value of productive HH assets (business or bus/private use)"
label var _x91009_a "Total value of agri HH assets"
label var _x91009aS "Share of agri HH assets value of total assets"


cd "$data"
save ${merge_`wave'}, replace
}
*



** MERGE INVESTMENT DATA **

//investment

cd "$cleandata_w3"
use invest.dta
drop if T!=1

recode _x62003a (1=1) (14/23=1) (5/7=2) (40/48=2), gen(agriinvest)

drop if _x62006a<2009
drop if _x62006a==2009 & _x62006<10

gen _x62007_c= _x62007 if agriinvest==1
gen _x62007_a= _x62007 if agriinvest==2


collapse (sum) _x62007 _x62007_a _x62007_c, by(QID)   
label var _x62007  "Total value of HH investment last 7 months"
label var _x62007_c  "Total value of HH crop related investment  last 7 months"
label var _x62007_a  "Total value of HH livestock related investment  last 7 months"

tostring QID , replace
save invest_temp, replace


cd "$data"
use $merge_w3, clear
merge 1:1 QID using "$cleandata_w3/invest_temp.dta"
drop if _merge==2 
drop _merge
erase "$cleandata_w3/invest_temp.dta"

replace _x62007=0 if _x62007==.
replace _x62007_a=0 if _x62007_a==.
replace _x62007_c=0 if _x62007_c==.


cd "$data"
save $merge_w3, replace



cd "$cleandata_w5"
use invest.dta
drop if T!=1

recode _x62003a (1=1) (14/23=1) (5/7=2) (40/48=2), gen(agriinvest)

drop if _x62006a<2012
drop if _x62006a==2012 & _x62006<10

gen _x62007_c= _x62007 if agriinvest==1
gen _x62007_a= _x62007 if agriinvest==2


collapse (sum) _x62007 _x62007_a _x62007_c, by(QID)   
label var _x62007  "Total value of HH investment last 7 months"
label var _x62007_c  "Total value of HH crop related investment  last 7 months"
label var _x62007_a  "Total value of HH livestock related investment  last 7 months"

tostring QID , replace force
save invest_temp, replace

cd "$data"
use $merge_w5, clear
merge 1:1 QID using "$cleandata_w5/invest_temp.dta"
drop if _merge==2 
drop _merge
erase "$cleandata_w5/invest_temp.dta"

replace _x62007=0 if _x62007==.
replace _x62007_a=0 if _x62007_a==.
replace _x62007_c=0 if _x62007_c==.


cd "$data"
save $merge_w5, replace




//disinvestment
foreach wave in w3 w5 {

cd "${cleandata_`wave'}"
use disinvest.dta

cap rename __62016 _x62016
cap rename __62017 _x62017

recode _x62016 (1=1) (14/23=2), gen(agrdenvest)

gen _x62017_a= _x62017 if agrdenvest<=2

collapse (sum)  _x62017_a, by(QID)   

label var _x62017_a  "Total value of HH agri devestment"

save disinvest_temp, replace

cd "$data"
use ${merge_`wave'}, clear
merge 1:1 QID using "${cleandata_`wave'}/disinvest_temp.dta"
drop if _merge==2 
drop _merge
erase "${cleandata_`wave'}/disinvest_temp.dta"

replace _x62017_a=0 if _x62017_a==.

cd "$data"
save ${merge_`wave'}, replace
}
*





*****************************************************************************


** MERGE BORROWING DATA **

foreach wave in  w5 w2 w3  {

cd "${cleandata_`wave'}"
use borr.dta

gen agriloans= 0
replace agriloans = _x71119b if _x71106a == 2 | _x71106b == 2 | _x71106a == 4 | _x71106b == 4


collapse  (sum) _x71119b agriloans, by(QID prov)

drop prov
save borr_temp, replace

cd "$data"
use ${merge_`wave'}, clear
merge 1:1 QID using "${cleandata_`wave'}/borr_temp.dta"
drop if _merge==2 
drop _merge
erase "${cleandata_`wave'}/borr_temp.dta"

replace _x71119b=0 if _x71119b==.
replace agriloans=0 if agriloans==.


gen agriloanD =(agriloans>0)


label var _x71119b "Total value of HH debt"
label var agriloans "Total value of HH debt related to agricultural expenses or investment"
label var agriloanD "HH has loan related to agricultural expenses or investment"


cd "$data"
save ${merge_`wave'}, replace
}
*


** MERGE MORE BORROWING DATA **

foreach wave in  w5 w2 w3  {

cd "${cleandata_`wave'}"
use borr.dta

gen baacloans= 0
replace baacloans = _x71119b if _x71109 == 52 

gen polloans= 0
replace polloans = _x71119b if _x71109 == 51 | _x71109 == 53 | _x71109 == 54 | _x71109 == 55 | _x71109 == 56 | _x71109 == 57 | _x71109 == 58 

gen takloans= 0
replace takloans = _x71119b if _x71109 == 60 

if "`wave'"=="w5"{
replace baacloans=.
replace polloans=.
replace takloans=.
}

collapse  (sum) baacloans polloans takloans, by(QID prov)

drop prov
save borr2_temp, replace

cd "$data"
use ${merge_`wave'}, clear
merge 1:1 QID using "${cleandata_`wave'}/borr2_temp.dta"
drop if _merge==2 
drop _merge
erase "${cleandata_`wave'}/borr2_temp.dta"

replace baacloans=0 if baacloans==.
replace polloans=0 if polloans==.
replace takloans=0 if takloans==.

gen baacloanD =(baacloans>0)
gen polloanD =(polloans>0)
gen takloanD =(takloans>0)


label var baacloans "Total value of HH debt borrowed from BAAC"
label var polloans "Total value of HH debt borrowed from any socio-political organiztaion"
label var takloans "Total value of HH debt borrowed from Taksin Village Fund"
label var baacloanD "HH has loan borrowed from BAAC"
label var polloanD "HH has loan borrowed from any socio-political organiztaion"
label var takloanD "HH has loan borrowed from Taksin Village Fund"

cd "$data"
save ${merge_`wave'}, replace
}
*




** MERGE SAVINGS DATA **

foreach wave in w2 w3 w5 {

cd "${cleandata_`wave'}"
use sav.dta

collapse (mean) _x71513 (sum) _x71514, by(QID)   
save sav_temp, replace

cd "$data"
use ${merge_`wave'}, clear
merge 1:1 QID using "${cleandata_`wave'}/sav_temp.dta"
drop if _merge==2 
drop _merge
erase "${cleandata_`wave'}/sav_temp.dta"

replace _x71513=1 if _x71513==1.5
replace _x71513=0 if _x71513==.
replace _x71514=0 if _x71514==.

label var _x71513  "Does HH have any kind of savings listed in the Qnr?"
label var _x71514  "Total value of HH savings"


//gen wealth vars
gen _wealth=  _x71514+ _x91009 - _x71119b

label var _wealth "Total assets  + total savings -total debt in HH"


cd "$data"
save ${merge_`wave'}, replace
}
*




*****************************************************************************



** MERGE FARMER INCOME GUARANTEE TRANSFER DATA **

cd "$cleandata_w3"
use transf.dta, clear

//rice insurance BURIRAM
gen riceisB_t =.
replace riceisB_t = 1 if __72102t== "agricultural products insurance project (rice, fruit)"
replace riceisB_t = 1 if __72102t== "income from agricultural products insurance project"
replace riceisB_t = 1 if __72102t== "insurance of income from agricultural products"
replace riceisB_t = 1 if __72102t== "rice insurance"
replace riceisB_t = 1 if __72102t== "insurance of rice's price"

gen riceisB_a = _x72103 if riceisB_t==1



//rice insurance WHOLE THAILAND (UPPER BOUND OF INDIVIDUALS receiving transfer) 
gen riceisT_t =riceisB_t 	//includes all rice transfers from Buriram
							//in other provs: rice transfer recorded under "other gov program"
							//so below I include all of these although half of them are probably "study for free" and "elictricity" program transfers
							//I try to exclude some of those below
replace riceisT_t = 1 if __72102t== "Other commune program"
replace riceisT_t = 1 if __72102t== "Other government program"
replace riceisT_t = 1 if __72102t== "other payments"
replace riceisT_t = 1 if __72102t== "other social assistence"
replace riceisT_t = 1 if __72102t== "other social security"

replace riceisT_t = . if _x72103 < 60 & prov!=31  	//we know from Buriram that rice insurance transfers are usually bigger than 60
													//so i exclude those here. this will take out probably most of the cases from the
													//"study for free" and 40% of the "electricity" programs
gen riceisT_a = _x72103 if riceisT_t==1
													
													
//rice insurance WHOLE THAILAND (LOWER BOUND OF INDIVIDUALS receiving transfer) 													
gen riceisTe_t =riceisT_t 							//to exclude all of the latter i increase the lower transf limit to 200
replace riceisTe_t = . if _x72103 < 200 & prov!=31

gen riceisTe_a = _x72103 if riceisTe_t==1


//disaster relief 
gen disarel_t =.
replace disarel_t = 1 if __72102t== "Social relief for natural disasters"
replace disarel_t = 1 if __72102t== "damage from flooding"
replace disarel_t = 1 if __72102t== "drought"
replace disarel_t = 1 if __72102t== "grant of flood insurance"
replace disarel_t = 1 if __72102t== "disaster relief package"

gen disarel_a = _x72103 if disarel_t==1



collapse  (mean) riceisB_t disarel_t riceisT_t riceisTe_t  (sum) riceisB_a disarel_a riceisT_a riceisTe_a , by(QID)

save transf_temp, replace

cd "$data"
use $merge_w3, clear
merge 1:1 QID using "$cleandata_w3/transf_temp.dta"
drop if _merge==2 
drop _merge
erase "$cleandata_w3/transf_temp.dta"

replace riceisB_t=0 if riceisB_t==. & prov==31
replace riceisB_a=0 if riceisB_a==. & prov==31
replace riceisT_t=0 if riceisT_t==.
replace riceisT_a=0 if riceisT_a==.
replace riceisTe_t=0 if riceisTe_t==.
replace riceisTe_a=0 if riceisTe_a==.
replace disarel_t=0 if disarel_t==.
replace disarel_a=0 if disarel_a==.

label var riceisB_t "Buriram..Indicator whether received rice insurance transfer"
label var riceisB_a "Buriram..Amount received rice insurance transfer"
label var riceisT_t "Thailand-upper bound of ind..Indicator whether received rice insurance transfer"
label var riceisT_a "Thailand-upper bound of ind..Amount received rice insurance transfer"
label var riceisTe_t "Thailand-lower bound of ind..Indicator whether received rice insurance transfer"
label var riceisTe_a "Thailand-lower bound of ind..Amount received rice insurance transfer"
label var disarel_t "Indicator whether received disaster relief transfer"
label var disarel_a "Amount received disaster relief transfer"



cd "$data"
save $merge_w3, replace



** MERGE FARMER INCOME GUARANTEE REGISTRATION DATA **


cd "/Users/`c(username)'/Dropbox/Research/DFG_FOR576/2. Original Data/Thai HH Survey 2013/4. Data clean/w5_v2/"
use rinsur_withmissing.dta, clear

*cd "$cleandata_w5"
*use rinsur.dta, clear

replace _x72110 = 2010 if 3109070406 & _x72112==.
replace _x72110=2009 if _x72110==2010 & prov==48
drop if QID =="4801100802" | QID =="4805110503"

gen received_sum= _x72113
gen received_min= _x72113

gen registered=1 if _x72112!=.

sort QID ID
bysort QID: gen year= _n
gen registered09=1 if _x72112!=. & year==1

cap rename yearnr year
gen whynotreg09=_x72111 if year==1

//did farmer recieve compensation in 2009/10 period
//all
recode __72110 (1 52  2552 5253 25552= 2009) (nonm=.), gen(year09)
tab year09
tab  _x72113 if year09==2009
drop year09
//Buriram, where years are mostly in tact
recode __72110 (1 52  2552 5253 25552= 2009) (nonm=.), gen(yearb)
replace yearb =. if prov!=31
tab  _x72113 if yearb==2009
drop yearb

collapse (sum) received_sum registered registered09 (min) received_min (firstnm) whynotreg09 ,by(QID prov)
replace registered=1 if registered>0 & registered!=.
replace registered09=1 if registered09>0 & registered09!=.
tab registered

replace received_min=0 if received_min >1


label define __72111 1 "did not know about it", modify
label define __72111 2 "forgot to do it", modify
label define __72111 3 "it was to much effort to go and register", modify
label define __72111 4 "do not trust the government", modify
label define __72111 5 "not satisfied in previous year", modify

label define __72111 10 "no farming/ rice farming", modify
label define __72111 11 "no land titel/person with land title registered", modify
label define __72111 12 "land is not enough/only ate rice but did not sell it", modify
label define __72111 13 "too complicated", modify
label define __72111 14 "The yield is not damaged/no drought", modify
label define __72111 15 "change of government", modify
label define __72111 16 "not willing to", modify

label values whynotreg09 __72111
		

cd "$cleandata_w5"
save rinsur_temp, replace

cd "$data"
use $merge_w5, clear
merge 1:1 QID using "$cleandata_w5/rinsur_temp.dta"
drop if _merge==2 
drop _merge
erase "$cleandata_w5/rinsur_temp.dta"

*replace registered=0 if registered==.

cd "$data"
save $merge_w5, replace




*****************************************************************************
*** SECTION 2.2d - CREATE PANEL  ********************************************


foreach wave in w1 w2 w3 w5 {

cd "$data"
use ${merge_`wave'}

** CORRECT VAR NAMES **
foreach var of varlist * {
rename  `var'  `wave'`var'
}

rename `wave'T T
rename `wave'QID QID
rename `wave'hhid hhid
rename `wave'prov prov
rename `wave'vill vill
rename `wave'distr distr
rename `wave'subdistr subdistr

cap destring prov, replace

cd "$data"
save ${merge_`wave'}, replace
}
*



** MERGE WAVES **
cd "$data"
use $merge_w2, clear
merge 1:1 QID hhid  using "$data/$merge_w3", force nogen
merge 1:1 QID hhid  using "$data/$merge_w5", force nogen
merge 1:1 hhid  using "$data/$merge_w1"

drop if _merge==2 //drop obs that only exist for w1 and no other wave
drop _merge


** MERGE EXPENDITURES **

merge 1:1 hhid  using "$cleandata_w5/cons_agg.dta",  nogen force keepusing(*total *cap_total)

** MERGE INCOME DATA**
//w1
merge 1:1 hhid using "$cleandata_w1/hhInc2.dta", keepusing(  __10080 __10081 __10082 __10083  __10084 __10085 __10086 __10087 __10088 __10091 __10092 __10093 __10094 __10100 __10101)
drop if _merge==2 //keep only hh which were existent in master data before merge
drop _merge
drop if T!=1

foreach root in 10080 10081 10082 10083   10084 10085 10086 10087 10088 10091 10092 10093 10094 10100 10101 {
cap rename  __`root' w1_x`root'
}
*

drop if QID=="41113090314"
drop if QID=="41117150209"

//w2 w3
merge 1:1 QID using "$cleandata_w3/hhinc_w3_w2.dta", keepusing(w2_x10080 w2_x10081 w2_x10082 w2_x10083 w2_x10084 w2_x10085 w2_x10086 w2_x10087 w2_x10088 w2_x10091 w2_x10092 w2_x10093 w2_x10094 w2_x10100 w2_x10101 _x10080 _x10081 _x10082 _x10083 _x10084 _x10085 _x10086 _x10087 _x10088 _x10091 _x10092 _x10093 _x10094 _x10100 _x10101)
drop if _merge==2 //keep only hh which were existent in master data before merge
drop _merge

foreach root in 10080 10081 10082 10083 10084 10085 10086 10087 10088 10091 10092 10093 10094  10100 10101 {
cap rename  _x`root' w3_x`root'
}
*
cd "$data"
saveold $dataset_v1, replace

//w5
cd "$cleandata_w5"
use hhinc.dta, clear
tostring QID, replace force
drop if T!=1
save hhinc_temp.dta, replace

cd "$data"
use $dataset_v1, clear
merge 1:1 QID using "$cleandata_w5/hhinc_temp.dta", keepusing(_x10080 _x10081 _x10082 _x10083 _x10084 _x10085 _x10086 _x10087 _x10088 _x10091 _x10092 _x10093 _x10094 _x10100 _x10101)
drop if _merge==2 //keep only hh which were existent in master data before merge
drop _merge

foreach root in 10080 10081 10082 10083 10084 10085 10086 10087 10088 10091 10092 10093 10094  10100 10101 {
cap rename  _x`root' w5_x`root'
}

cd "$cleandata_w5"
erase hhinc_temp.dta
*

** ALTERNATIVE INCOME **
drop *10088 *10087
rename *10088new *10088
rename *10087new *10087

//creat income occurance dummies
foreach var of varlist  w1_x1008* w2_x1008* w3_x1008* w5_x1008*   *_x10093 {
gen `var'_D = (`var'!=0) if `var'!=.
}

** REPAIRE **
drop  w1_x31005b w1_x41009a  w1ownlandv w1_x43103a w1seedsres 
drop if hhid==4578
drop if hhid==4577
rename w5registered w3registered
rename w1_x43109a w1_x43109
destring vill, replace


** W1 PPP CONVERSION **
foreach var in  w1_x31005 w1_x31006 w1_x10080 w1_x10081 w1_x10082 w1_x10083 w1_x10084 w1_x10085 w1_x10086 w1_x10087 w1_x10088 w1_x10091 w1_x10092 w1_x10093 w1_x10094 w1_x10100 w1_x10101{
cap confirm variable `var'
if !_rc {
replace `var'=`var'/17.17 
}
}
*


*****************************************************************************

note: "JZ: v1: completed data set, ready for analysis. cleaned and megred with hh data variables. merged with village level data. all village level var names are indicated with v_..."

destring subdistr, replace
label values subdistr subdistr


cd "$data"
saveold $dataset_v1, replace


erase $merge_w1
erase $merge_w2
erase $merge_w3
erase $merge_w5

* to do: write protect data  

* smth wrong with w5_x10093 and w1expend

*NOTE: var names cannot have more than 9 digits!


