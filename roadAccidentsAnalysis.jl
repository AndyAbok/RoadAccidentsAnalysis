using DataFrames
using XLSX
using FloatingTableView
using Dates
using Plots
using HypothesisTests
using Distributions
using VegaLite

include("libraries.jl")

roadData2016 = DataFrame(XLSX.readtable("kenya-accidents-database.xlsx","2016",infer_eltypes=true)...)
roadData2017 = DataFrame(XLSX.readtable("kenya-accidents-database.xlsx","2017",infer_eltypes=true)...)

##Data Cleaning 
# 1.) Unify the column names to be able to merge the data 
cols1 = names(roadData2016)
cols2 = names(roadData2017)

diffCols = cols2[cols2 .∉ Ref(cols1)]
upDateRoadData2017 = select(roadData2017,Not(diffCols))
finalRoadData = vcat(roadData2016,upDateRoadData2017)

# 2.) Alighning the data types and handling missing values 
eltype.(eachcol(finalRoadData))
filter(ismissing,finalRoadData[:,Symbol("TIME 24 HOURS")])   

#=
finalRoadData[!,Symbol("TIME 24 HOURS")]  = replace(finalRoadData[:,Symbol("TIME 24 HOURS")],missing => 0000)
finalRoadData[!,Symbol("TIME 24 HOURS")] = [x isa AbstractString ? parse(Int, x) : Int(x) for x in roadData[!,:1]]
=#

finalRoadData = dropmissing(finalRoadData)

"""
a.) conversion function to time,b.)Remove String/Characters in the time col
"""

toTimeConverter(x::Integer) = Time(divrem(x, 100)...)

function removeUnwantedChar(input::Any)
    reducedStr = 
        if length(input) > 4
            deleteat!(collect(input),5:length(input)) |> String
        elseif occursin("H",string(input))
            idx = findfirst("H",input)
            deleteat!(collect(input),idx) |> String
        else
           input
        end
      return reducedStr
end
  

finalRoadData[!,Symbol("TIME 24 HOURS")]  = ifelse.(finalRoadData[:,Symbol("TIME 24 HOURS")] .== "UNKNOWN TIME", "0000", finalRoadData[:,Symbol("TIME 24 HOURS")])
finalRoadData[!,Symbol("TIME 24 HOURS")]  = ifelse.(finalRoadData[:,Symbol("TIME 24 HOURS")] .== "UNKNOWN", "0000", finalRoadData[:,Symbol("TIME 24 HOURS")])
finalRoadData[!,Symbol("TIME 24 HOURS")]  = removeUnwantedChar.(finalRoadData[!,Symbol("TIME 24 HOURS")])
finalRoadData[!,Symbol("TIME 24 HOURS")]  = removeUnwantedChar.(finalRoadData[!,Symbol("TIME 24 HOURS")])
finalRoadData[!,Symbol("TIME 24 HOURS")] = [x isa AbstractString ? parse(Int, x) : Int(x) for x in finalRoadData[!,:1]]
finalRoadData[!,Symbol("TIME 24 HOURS")] = toTimeConverter.(finalRoadData[:,:1]) 

#3.) Renaming the columns to better names 
newColNames = [:Time,:subCounty,:County,:Road,:Place,:Details,:Gender,:Age,:CauseCode,:Victim,:Number,:Date]
finalRoadData = rename(finalRoadData,names(finalRoadData) .=> newColNames)

### Data Crunching 
# 1.) analyze accidents by time of occurance 
accidentByHour = 
         finalRoadData |> 
         finalRoadData -> combine(groupby(transform(finalRoadData,[:Time,:Date] .=> [ByRow(hour),ByRow(year)]),
                                  [:Time_hour]),:Time_hour .=> length => :accidentsByHour) 
               

Plots.bar(accidentByHour[:,:Time_hour],
    accidentByHour[:,:accidentsByHour],
    title  = "Number of Accidents Per Hour in 2016/2017",
    xlabel = "Hour",
    ylabel = "Count Price",
    label="NumberOfAccidents",
    size = (1000,500),
    legend = :outertopright)

accidentsByMonth = 
        finalRoadData |> 
        finalRoadData -> combine(groupby(transform(finalRoadData,[:Date,:Time] .=> [ByRow(month),ByRow(hour)]),
                         [:Date_month,:Time_hour]),:Date_month .=> length => :monthlyHourlyAccidents)                      

data2 = accidentsByMonth[:,:1]
df2 = DataFrame(map(idx -> getindex.(data2, idx), eachindex(first(data2))),:auto)
df2.date = map(x-> Date.(Month(x)), df2.x1)
df2.month = map(y-> Dates.monthname.(Date.(Month(y))), df2.x1) 
cleanMonthlyData = hcat(df2[:,[:3]],accidentsByMonth[:,:2:end])

cleanMonthlyData |>
    @vlplot(
        title="Total Accidents By Month and Hour",
        :rect,
        x={
            "Time_hour:o",            
            title="hour",            
        },
        y={
            "month:o",          
            title="Month"
        },width=800,height=700,
        color={
            "monthlyHourlyAccidents:q",
            aggregate="sum",
            legend={title=nothing}
        },
        config={
            view={
                strokeWidth=0,
                step=13
            },
            axis={
                domain=false
            }
        }
    )

accidentsByDayAndHr = 
        finalRoadData |> 
        finalRoadData -> combine(groupby(transform(finalRoadData,[:Date,:Time] .=> [ByRow(dayname),ByRow(hour)]),
                         [:Date_dayname,:Time_hour]),:Date_dayname .=> length => :monthlyHourlyAccidents)

accidentsByDayAndHr |>
    @vlplot(
        :circle,
        y="Date_dayname:o",
        x="Time_hour:o",
        size="sum(monthlyHourlyAccidents)",
        width=600,height=600
    )

aggregateDay = 
        finalRoadData |> 
        finalRoadData -> combine(groupby(transform(finalRoadData,[:Date,:Time] .=> [ByRow(dayname),ByRow(hour)]),
                         [:Date_dayname]),:Date_dayname .=> length => :TotalByDay)


"""
conduct the kruskal wallis test 
This is done to test for seasonality 
H₀ : The distribution of the nummber of accidents are the same for each hour of the day(i.e the time of the day does not affect the number of accidents) 
the alternative hypotesis 
H₁ : There are hours during the day for which the distribution of the number of accidents differ significatly
let n = n₀ + n₁ + ... + n23 denote the sample size which is divided into 24 disjoint groups of size n₀ = n₁ = ... = n23(group size correspnd to the respective hours of the day )   
Each group is randomly selected from a diffrent population.The entire sample(all groups together) is ranked
The Test statistics is give by the formulae T = 12/n(n + 1) ∑(R̄ᵢ - n + 1/2)^2 nᵢ 
where R̄ᵢ = 1/nᵢ ∑R_ij 
"""

grps = accidentByHour[:,:Time_hour]
values = accidentByHour[:,:accidentsByHour]
kWallisRes = KruskalWallisTest(grps,values)


#2.)Analysis of accidents by county and subcounty
finalRoadData[!,:County] = lowercase.(finalRoadData[!,:County]) |> x -> uppercasefirst.(x)

noOfAccidentsByCounty = 
    finalRoadData |>
    finalRoadData -> combine(groupby(finalRoadData,:County),:County .=> length .=> :accidentsByCounty) |>
    finalRoadData -> sort(finalRoadData,:2,rev = true) |>
    finalRoadData -> first(finalRoadData,10)

noOfAccidentsByCounty |> @vlplot(:bar,y=:County,x=:accidentsByCounty,width=700,height=500)

accidentsBySubCounty = 
    finalRoadData |>
    finalRoadData -> filter(:County => ==("Nairobi"),finalRoadData)|>
    finalRoadData -> combine(groupby(finalRoadData,:2),:2 .=> length => :accidentsBySubCounty) |>
    finalRoadData -> sort(finalRoadData,:2,rev = true) 
    

accidentsBySubCounty |>
    @vlplot(
        height={step=12},
        :bar,
        transform=[
            {
                window=[{op="sum",field="accidentsBySubCounty",as="totalAcidents"}],
                frame=[nothing,nothing]
            },
            {
                calculate="datum.accidentsBySubCounty/datum.totalAcidents * 100",
                as="PercentOfTotal"
            },
            {
            window=[{ op="rank", as="rank" }],
            sort=[{ field="accidentsBySubCounty", order="descending" }]
            },
             {filter="datum.rank <= 10"}
        ],
        x={"PercentOfTotal:q", axis={title="% of total Time"}},
        y={"subCounty:n"},
        width=700,height=500
    )


# Narrow Down to details i.e cause of the accidents in the various sub county 
finalRoadData[!,:Details] = replace.(finalRoadData[:,:Details], "&" .=> "AND")
finalRoadData[!,:Details] = replace.(finalRoadData[:,:Details], "HIT AND RAN" .=> "HIT AND RUN")
finalRoadData[!,:Details] = lowercase.(finalRoadData[!,:Details]) |> x -> uppercasefirst.(x)

selectedSubCounties = first(accidentsBySubCounty,10) |> local data -> data[:,:subCounty]

noOfAccidentsByDetails = 
    finalRoadData |>
    finalRoadData -> filter(:County => ==("Nairobi"),finalRoadData)|>
    finalRoadData -> filter(x -> x.subCounty in selectedSubCounties, finalRoadData) |>
    finalRoadData -> combine(groupby(finalRoadData,[:subCounty,:Details]),:Details .=> length .=> :NoByDetails) |>
    finalRoadData -> sort(finalRoadData,[:1,:3],rev = (true,true)) 
    #finalRoadData -> first(finalRoadData[:,:Details],5)
    #finalRoadData -> unique(finalRoadData,:subCounty)
    #finalRoadData -> first(finalRoadData,20)

noOfAccidentsByDetails |>
     @vlplot(:bar, x="NoByDetails", y=:subCounty, color=:Details)


countyAndDetails = 
    finalRoadData |>
    finalRoadData -> filter(:County => ==("Nairobi"),finalRoadData)|>
    finalRoadData -> combine(groupby(finalRoadData,:Details),:Details .=> length .=> :ByDetails) |>
    finalRoadData -> sort(finalRoadData,:2,rev = true) |>
    finalRoadData -> first(finalRoadData,10) 

countyAndDetails |>
    @vlplot(
        transform=[
            {filter="datum.ByDetails != null"},
            {
                joinaggregate= [{
                    op=:mean,
                    field=:ByDetails,
                    as="AverageAccidents"
                }]
            }           
        ]
    ) +
    @vlplot(
        :bar,
        x={"ByDetails:q",axis={title="Number Of Accidnets"}},width=800,height=500,
        y={"Details:o"}
    ) +
    @vlplot(
        mark={
            :rule,
            color="red"
        },
        x={"AverageAccidents:q", aggregate="average"}
    )

#TODO: add a pie chart here 

#3.) Analysis by Road 

finalRoadData[!,:Road] = replace.(finalRoadData[!,:Road], "-" .=> " ", count=1)

noOfAccidentsByRoad = 
    finalRoadData |>
    finalRoadData -> combine(groupby(finalRoadData,:Road),:Road .=> length .=> :accidentsByRoad) |>
    finalRoadData -> sort(finalRoadData,:2,rev = true)
   

noOfAccidentsByRoad |>
    @vlplot(
        title="Number of Accidents By Road",
        transform=[
            {
                aggregate=[{op="mean",field="accidentsByRoad",as="aggregate_accidents"}],
                groupby=["Road"]
            },
            {
                window=[{op="row_number", as="rank"}],
                sort=[{field="aggregate_accidents",order="descending"}]
            },
            {
                calculate="datum.rank < 10 ? datum.Road : 'All Others'", as="ranked_ByRoad"
            }
        ],
        :bar,
        x={aggregate="mean","aggregate_accidents:q",title=nothing},width=800,height=500,
        y={
            sort={op="mean",field="aggregate_accidents",order="descending"},
            "ranked_ByRoad:o",
            title=nothing
        }
    )


noOfAccidentsByRoadAndDetail = 
    finalRoadData |>
    finalRoadData -> filter(:Road => in(Set(["NAIROBI MOMBASA","MOMBASA NAIROBI"])),finalRoadData)|>
    finalRoadData -> combine(groupby(finalRoadData,[:Details]),:Details => length .=> :accidentsByRoadAndDetails) |>
    finalRoadData -> sort(finalRoadData,:2,rev = true)|>
    finalRoadData -> first(finalRoadData,10)
    
noOfAccidentsByRoadAndDetail |>
    @vlplot(
        width=200,
        height={step=16},
        y={:Details,axis=nothing},width=800,height=800
    ) +
    @vlplot(
        mark={:bar,color="#e7ba52"},
        x={"accidentsByRoadAndDetails",axis={title="accidents By Road And Details"},scale={domain=[0,100]}},           
    ) +
    @vlplot(
        mark={:text,align="left",x=5},
        text="Details:n"
        #detail={aggregate="count",type="quantitative"}
    )


#4.) Analysis By Victim
noOfAccidentsByVictim = 
    finalRoadData |>
    finalRoadData -> combine(groupby(finalRoadData,:Victim),:Victim .=> length .=> :accidentsByVictims) |>
    finalRoadData -> sort(finalRoadData,:2,rev = true) |>
    finalRoadData -> first(finalRoadData,5)

labels = noOfAccidentsByVictim[:,:Victim]
noOfAccidentsPerVictim = noOfAccidentsByVictim[:,:accidentsByVictims]

###############
p = PyPlot.pie(noOfAccidentsPerVictim,
                labels=labels,
                shadow=true,
                startangle=90,
                autopct="%1.1f%%")		
title("Total Accidents Per Victim")
figure("pyplot_piechart",figsize=(10,10))

#5.) Analysis By Age Group
#=
let 1 = male 
let 2 = female 
=#

map(eachrow(finalRoadData)) do r
    if r.Gender .== "M" 
       r.Gender = 1
    elseif r.Gender .== "F"
           r.Gender = 2
    else
         r.Gender = 3
    end
end

accidentByGender = 
    finalRoadData |>    
    finalRoadData -> combine(groupby(finalRoadData,:Gender),:Gender .=> length => :accidentsByGender)  

accidentByGender[!,:inPercentage]  = accidentByGender[:,:accidentsByGender] ./sum(accidentByGender[:,:accidentsByGender]) .* 100.0

#Calculate the relative risk of which geneder is more exposed to accidents
finalRoadData[!, :AgeGroup] = Vector{Union{String, Nothing}}(nothing, size(finalRoadData, 1))

map(eachrow(finalRoadData)) do r

    if r.Age in 16:19  
       r.AgeGroup = "16-19"

    elseif r.Age in 20:24 
           r.AgeGroup = "20-24"

    elseif r.Age in 25:29 
           r.AgeGroup = "25-29"

    elseif r.Age in 30:34 
           r.AgeGroup = "30-34"

    elseif r.Age in 35:39 
           r.AgeGroup = "35-39"

    elseif r.Age in 40:44 
           r.AgeGroup = "40-44"

    elseif r.Age in 45:49 
           r.AgeGroup = "45-49"

    elseif r.Age in 50:54 
           r.AgeGroup = "50-54"

    elseif r.Age in 55:59 
           r.AgeGroup = "55-59"

    elseif r.Age in 60:64 
           r.AgeGroup = "60-64"

    elseif r.Age in 65:69 
           r.AgeGroup = "65-69"

    elseif r.Age in 70:100 
           r.AgeGroup = "70-100"

    else
         r.AgeGroup = "Other"
    end
end

accidentByAge = 
    finalRoadData |>    
    finalRoadData -> combine(groupby(finalRoadData,[:AgeGroup]),:AgeGroup .=> length => :accidentsByAge) 

accidentByAge |>
    @vlplot(
        :line,
        transform=[
            {filter="datum.AgeGroup !=='Other'"}
        ],
        x=:AgeGroup,
        y=:accidentsByAge
    )

accidentByAgeAndGender = 
    finalRoadData |>    
    finalRoadData -> combine(groupby(finalRoadData,[:AgeGroup,:Gender]),:AgeGroup .=> length => :accidentsByAgeGender) 

accidentByAgeAndGender |>
    @vlplot(
        :line,
        transform=[
            {filter="datum.AgeGroup !=='Other'"}
               ],
        x=:AgeGroup,
        y=:accidentsByAgeGender,
        color=:Gender
    )


```
This represents relative risk for men and women of being involved in an accident for each of the AgeGroups.
In this case, relative risk is calculated by dviding each genders share of involvements by
its share of Age Group numbers. A relative risk of 1.0 indicates no difference in
risk of involvement between the Age group and the overall Gender Involvement. Relative risk
values over 1.0 indicate overinvolvement, and values less than 1.0 indicate
underinvolvement.

```
# Accidents by gender and hour 

accidentByHourGender = 
         finalRoadData |> 
         finalRoadData -> combine(groupby(transform(finalRoadData,[:Time,:Date] .=> [ByRow(hour),ByRow(year)]),
                                  [:Time_hour,:Gender]),:Time_hour .=> length => :accidentsByHourGender)

accidentByHourGender |>
    @vlplot(
        :bar,
        column="Time_hour:o",
        y={"accidentsByHourGender", axis={title="population", grid=false}},
        x={"Gender:n", axis={title=""}},
        color={"Gender:n"},
        spacing=10,
        config={
            view={stroke=:transparent},
            axis={domainWidth=1}
        }
    )


probabilityByMonth = 
        finalRoadData |> 
        finalRoadData -> combine(groupby(transform(finalRoadData,[:Date,:Time] .=> [ByRow(month),ByRow(hour)]),
                         [:Date_month,:Gender]),:Date_month .=> length => :countByMonthGender)


subData = probabilityByMonth[:,:countByMonthGender]

probsPerGender = StatsBase.zscore(subData,mean(subData),std(subData))

###### PREDICTIBE ANALYTICS USING REGRESSION #### 

#=
response Variable = Gender
Against 
Time of Day
County
Victim 
Age Group 
=#

regressionData = 
    finalRoadData |>
    finalRoadData -> select(finalRoadData,[:Time,:County,:AgeGroup,:Gender]) |>
    finalRoadData -> transform(finalRoadData,:Time =>  ByRow(hour),renamecols = false) 

#Convert string categorical data into numeric categorical data
function ConvertData(data::DataFrame,ColName::String)      
    index = findfirst(names(data) .== ColName)
 
    uniqueVar = unique(data[:,index])
    n = Float64.(collect(1:length(uniqueVar)))
    dict_info = Dict(map((x,y) -> (x,y),uniqueVar,n))
    data[!,:newCol] = Vector{Union{Int64, Nothing}}(nothing, size(data, 1))

    for (v,service) in enumerate(data[:,index])
            data[v,:newCol]= dict_info[service]
         end

    omitStringCol = select(data,Not(ColName))
    rename!(omitStringCol,:newCol => ColName)
    
    return omitStringCol,dict_info
end


function splitdf(df, pct)
    @assert 0 <= pct <= 1
    ids = collect(axes(df, 1))
    shuffle!(ids)
    sel = ids .<= nrow(df) .* pct
    trainset =   [df, sel, :]
    testset =  [df, .!sel, :]
    return trainset[1],testset[1]
end

regressionDataUp,countyCodeKey =  ConvertData(regressionData,"County")
regressionDataUp2,ageGroupKeys =  ConvertData(regressionDataUp,"AgeGroup")

transform!(regressionDataUp2,names(regressionDataUp2) .=> ByRow(Float64),renamecols=false)
train,test =  splitdf(regressionDataUp2, 0.7)

##Build the model
ols = lm(@formula(Gender ~ Time + AgeGroup + County), train)
r2(ols)

gm18 = fit(GeneralizedLinearModel, @formula(Gender ~ County+Time+AgeGroup), train, NegativeBinomial(2.0), LogLink())
 
dof(gm18)
deviance(gm18)
aic(gm18)
bic(gm18)
coef(gm18)[1:4]


# toTimeSeriesData = 
#      finalRoadData|>
#      finalRoadData -> combine(groupby(finalRoadData,[:Date]),:Date .=> length => :noOfAccidents) 

# toTimeSeriesData[!,:noOfAccidents] = Float64.(toTimeSeriesData[:,:noOfAccidents])




                                