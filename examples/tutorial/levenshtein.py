import sys 
def levenshteinDistance(f1_name, f2_name):
    f1 = open(f1_name, 'r')
    s1 = f1.read()
    f2 = open(f2_name, 'r')
    s2 = f2.read()
    if len(s1) > len(s2):
        s1,s2 = s2,s1
    distances = range(len(s1) + 1)
    for index2,char2 in enumerate(s2):
        newDistances = [index2+1]
        for index1,char1 in enumerate(s1):
            if char1 == char2:
                newDistances.append(distances[index1])
            else:
                newDistances.append(1 + min((distances[index1],
                                             distances[index1+1],
                                             newDistances[-1])))
        distances = newDistances
    f1.close()
    f2.close()
    return distances[-1]

if __name__ == "__main__":
   print levenshteinDistance(sys.argv[1], sys.argv[2])
