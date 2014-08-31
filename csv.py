import csv

with open('C:/Users/gaoxinyang/Desktop/data science/data.txt','r') as csvinput1:
    
    with open('C:/Users/gaoxinyang/Desktop/data science/output.txt', 'w') as csvoutput:
        writer = csv.writer(csvoutput, lineterminator='\n')
        all = []
        for row in csv.reader(csvinput1,delimiter='\t'):
            country = row[1]
            print(country)
            with open('C:/Users/gaoxinyang/Desktop/data science/gdp.csv','r') as csvinput2:
                for row1 in csv.reader(csvinput2,delimiter=','):
                    country1 = row1[0]
                    if country.lower() == country1.lower():
                        row.append(row1[len(row1)-3])
                        all.append(row)
        writer.writerows(all)
                    
            
      
        
