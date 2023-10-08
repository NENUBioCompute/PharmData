# -*- coding: utf-8 -*-
"""
# @Time        : 2023/10/8
# @Author      : yvshilin
# @Email        : 3542804395@qq.com
# @FileName    : ChEMBLParser.py
# @Software    : PyCharm
# @ProjectName : ChEMBL
"""

import csv

class FileAnalysis:
    def TsvToCsv(self, tsvFile, csvFile):
        with open(tsvFile, 'r') as file:
            tsvReader = csv.reader(file, delimiter='\t')
            with open(csvFile, 'w') as outfile:
                csvWriter = csv.writer(outfile)
                for row in tsvReader:
                    csvWriter.writerow(row)

    def ExtractColumn(self, inputFile, outputFile, columnsToExtract):
        with open(inputFile, 'r') as inputCsv:
            inputReader = csv.reader(inputCsv)
            with open(outputFile, 'w') as outputCsv:
                outputWriter = csv.writer(outputCsv)
                for row in inputReader:
                    extractedData = [row[i] for i in columnsToExtract]
                    outputWriter.writerow(extractedData)

