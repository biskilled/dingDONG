import csv, json, io, collections

filePath = 'C:\\temp\\lior.csv'
jsonPath = 'C:\\temp\\liorHamanyak.json'

tempCol = {}
with io.open( filePath, 'r', encoding='utf-8') as textFile:
    fFile = csv.reader(textFile)
    for i, split_line in enumerate(fFile):
        c = split_line[0]
        cc = split_line[1]

        if c not in tempCol:
            tempCol[c] = []

        tempCol[c].append (cc)

with open (jsonPath, "w", encoding="utf-8") as jf:
    json.dump(tempCol, jf, ensure_ascii=False, indent=4)
