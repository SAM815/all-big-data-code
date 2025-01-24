# - [ ] a) PySpark code to find the total number of people earning over 50k with bachelor, master and HS-grad


import sys

from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    sc = SparkContext("local","Pyspark Practice Problem")

    lines = sc.textFile("/input/input.txt").map(lambda line: line.split(","))

    lines = lines.map(lambda x:((x[3],(x[14],1)) if x[14] == ' >50K' else  (x[3],(x[14],0))))

    lines = lines.filter(lambda x:(x[0] == ' Bachelors' or x[0] == ' Masters' or x[0] == ' HS-grad'))

    lines = lines.reduceByKey(lambda x,y: (x[0],x[1]+y[1])).mapValues(lambda x: x[1])

    lines.saveAsTextFile("/output")



#     1. age: continuous.
# 2. workclass: Private, Self-emp-not-inc, Self-emp-inc, Federal-gov, Local-gov, State-gov, Without-pay, Never-worked.
# 3. fnlwgt: continuous.
# 4. education: Bachelors, Some-college, 11th, HS-grad, Prof-school, Assoc-acdm, Assoc-voc, 9th, 7th-8th, 12th, Masters, 1st-4th, 10th, Doctorate, 5th-6th, Preschool.
# 5. education-num: continuous.
# 6. marital-status: Married-civ-spouse, Divorced, Never-married, Separated, Widowed, Married-spouse-absent, Married-AF-spouse.
# 7. occupation: Tech-support, Craft-repair, Other-service, Sales, Exec-managerial, Prof-specialty, Handlers-cleaners, Machine-op-inspct, Adm-clerical, Farming-fishing, Transport-moving, Priv-house-serv, Protective-serv, Armed-Forces.
# 8. relationship: Wife, Own-child, Husband, Not-in-family, Other-relative, Unmarried.
# 9. race: White, Asian-Pac-Islander, Amer-Indian-Eskimo, Other, Black.
# 10. sex: Female, Male.
# 11. capital-gain: continuous.
# 12. capital-loss: continuous.
# 13. hours-per-week: continuous.
# 14. native-country: United-States, Cambodia, England, Puerto-Rico, Canada, Germany, Outlying-US(Guam-USVI-etc), India, Japan, Greece, South, China, Cuba, Iran, Honduras, Philippines, Italy, Poland, Jamaica, Vietnam, Mexico, Portugal, Ireland, France, Dominican-Republic, Laos, Ecuador, Taiwan, Haiti, Columbia, Hungary, Guatemala, Nicaragua, Scotland, Thailand, Yugoslavia, El-Salvador, Trinadad&Tobago, Peru, Hong, Holand-Netherlands.
#15. income: >50K, <=50K