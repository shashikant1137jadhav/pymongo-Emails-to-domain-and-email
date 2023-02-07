from pymongo import MongoClient
import sys
import re



try:
    dbClient = MongoClient(
        "mongodb://localhost:27017"
    )
    print("\nConnected to database.")
except Exception:
    print("\nUnable to connect to the database server.\n")
    sys.exit(1)

#to match
regex = '^[_a-z0-9-]+(\.[_a-z0-9-]+)*@[a-z0-9-]+(\.[a-z0-9-]+)*(\.[a-z]{2,})$'

#Collection1 always contain raw emails
#collection2  contains main files
#we want to apply lowercase for everything


db = dbClient.Email
collection1 = db.all_emails
collection2=db.Domain_email

query1 = {'Email':{"$exists":True}}
query2={'Domain':{'$exists':True}}

Domain2_lst =[]
for doc2 in collection2.find(query2, {'Domain': 1, 'Email': 1}):
    Domain2 = doc2['Domain']
    Email2=doc2['Email']
    Domain2_lst.append(Domain2)

doc_count=0
for doc1 in collection1.find(query1,{'Email': 1}):
    doc_count=doc_count+1
    print(doc_count)
    Email1=doc1['Email']
    Email1 = str(Email1).lower()
    match = re.match(regex, Email1)
    if match == None:
        print(f'{Email1} --- Bad Syntax')
        continue
    lst1=Email1.split('@')
    Domain1=lst1[1]

    if Domain1 in Domain2_lst:
        Email2_lst=[]
        query3={'Domain':Domain1}
        for doc3 in collection2.find(query3, { 'Email': 1,'count':1}):
            Email2=doc3['Email']
            count2=doc3['count']
            #Email2_lst.extend((Email2))


            if Email1 not in Email2_lst:
               # print(Email1)
                #Email2_lst.append(Email1)
                #print(Email2_lst)

                collection2.update_one(
                    {'Domain':Domain1},
                    {"$push":
                        {
                            'Email' : Email1
                        }
                    })

                count2=count2+1
                collection2.update_one(
                    {'Domain': Domain1},
                    {"$set":
                        {
                            'count': count2
                        }
                    })



    else:
        my_dict = {'Domain': lst1[1], 'Email': [Email1],'count':1}
        collection2.insert_one(my_dict)
        Domain2_lst.append(lst1[1])
