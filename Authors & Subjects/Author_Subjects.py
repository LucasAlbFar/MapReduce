import mincemeat
import glob
import csv

text_files = glob.glob('D:\\Big Data\\PUC\\Python27\\01 PUC Trab2.3\\*')

def file_contents(file_name):
    f = open(file_name)
    try:
        return f.read()
    finally:
        f.close()

source = dict((file_name, file_contents(file_name))for file_name in text_files)

def mapfn(k, v):
    from stopwords import allStopWords

    for line in v.splitlines():
        elemts = line.split(':::')
        auts = elemts[1].split('::')
        book = elemts[2].split(' ')
        size_book = len(book)

        if size_book > 1:
            for i in range(size_book):
                book[i] = book[i].upper()

            book[size_book-1] = book[size_book-1].replace('.','')

            for author in auts:
                for word in book:
                    if (word not in allStopWords):
                        k = author + ':' + word
                        yield k, 1
                        print ('map ' + k, 1)
                    

            
def reducefn(k, v):
    print ('reduce '+ k)
    return sum(v)

s = mincemeat.Server()
s.datasource = source
s.mapfn = mapfn
s.reducefn = reducefn

results = s.run_server(password="changeme")

w = csv.writer(open("D:\\Big Data\\PUC\\Python27\\01 PUC Trab2.3\\RESULTADO.csv", "w"))
for k, v in results.items():
    w.writerow([k,v])