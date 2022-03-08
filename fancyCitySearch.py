from redisearch import Client, IndexDefinition, GeoFilter, GeoField, Client, Query, TextField, NumericField
import redis, re
from redisearch.client import TagField


def printResult(comment, res, query):
    print('\n********\n'+comment)
    print('\tCharacters submitted: -->  '+query.query_string())
    
    if(res.total>0):
        print('\ntotal # results: ' +str(res.total)) 
        print(res.docs)
        #print('city: ' +res.docs[0].city)
        #print('state: ' +res.docs[0].state)
    elif(res.total<1):
        print('no result for that ^ query\n')
        #print('score: ' +str(res.docs[0].score))

client = Client("idx:cities:test_index")

try:
    client.drop_index()
except:
    print('index does not exist yet - no worries...')

# IndexDefinition auto-indexes any hashes with keys that have a matching prefix
definition = IndexDefinition(prefix=['addr:'],language='English')

# Creating the index definition and schema
client.create_index(
(
    TextField("city", weight=500.0, phonetic_matcher='dm:en'), 
    TagField('state',),# weight=100.0,separator=','),
    TagField('zip_postal'),# weight=50.0),
    TagField('country'),# weight=10.0),
    TextField("search_terms_city", weight=500.0, phonetic_matcher='dm:en')
    #GeoField('loc')
),
stopwords = [],
definition=definition)


# Indexing a document for RediSearch 2.0+
client.redis.hset('addr:1',
                mapping={
                    'city': 'San Francisco',
                    'state': 'CA,C A,C.A.,California',
                    'zip_postal': '94016',
                    'country': 'USA,United States,United States of America,America,U.S.A',
                    'search_terms_city': 'SFO,S.F.O.,SF,San Frn',
                    'loc': "-122.419,37.774"
                })
client.redis.hset('addr:2',
                mapping={
                    'city': 'Los Angeles',
                    'state': 'CA,C A,C.A.,California',
                    'zip_postal': '90001',
                    'country': 'USA,United States,United States of America,America,U.S.A',
                    'search_terms_city': 'Los Angls,LA,Los Angels',
                    'loc': "-118.129,33.897"
                })
client.redis.hset('addr:3',
                mapping={
                    'city': 'San Jose',
                    'state': 'CA,C A,C.A.,California',
                    'zip_postal': '94088',
                    'country': 'USA,United States,United States of America,America,U.S.A',
                    'search_terms_city': 'SanJose',
                    'loc': "-121.859,37.291"
                })
client.redis.hset('addr:4',
                mapping={
                    'city': 'Albuquerque Albuquerque',
                    'state': 'NM,New Mexico',
                    'zip_postal': '87101',
                    'country': 'USA,United States,United States of America,America,U.S.A',
                    'search_terms_city': 'Abq,Albekerke',
                    'loc': "-106.640,35.064"
                })

# Add synonyms  (single word/token only: that is given equal value as the alternate provided token)
client.redis.execute_command('FT.SYNUPDATE','idx:cities:test_index','LA_Group','COA','LA')
client.redis.execute_command('FT.SYNUPDATE','idx:cities:test_index','SJ_Group','song','SanJose')
client.redis.execute_command('FT.SYNUPDATE','idx:cities:test_index','ABQ_Group','Duke','Museum','Albuquerque')


# using SCORER TFIDF.DOCNORM we can tell when different attributes are matched on:
# if we set the weights as follows:
# ct  = city --> 500
# st = state --> 100
# zp = zip_postal --> 50
# c = country --> 10
# Then the following combos would exist:
# ct == 500, ct + st = 600, ct + zp = 550, ct + c = 510
# st = 100, st + zp = 150, st + c = 110
# zp = 50, zp + c = 60
# c = 10
def redis_search(city, state, zip_postal, ctry):
    print(city,' ', state, ' ', zip_postal)

    if city is None or city == '':
        city = 'XXXXX'
    else:
        city = re.sub(r'[^a-zA-Z0-9]','', city)
    if state is None or state == '':
        state = 'XX'

    if zip_postal is None or zip_postal == '' :
        zip_postal = 'XXXXX'
    elif ctry == 'USA' and len(str(zip_postal)) > 5:
        zip_postal = str(zip_postal)[0:5]
    else:
        zip_postal = re.sub(r'[^a-zA-Z0-9]', '', zip_postal)

    city = str(city)
    state = str(state)

    res = client.search(Query("@country: " + ctry + " @city: (%%" + city + "%%|" + city + "* ) @state: " + state
                              + " @postalcode:" + zip_postal).with_scores())
    if len(res.docs) > 0:
        res.docs[0].flag = 'A'
        return res.docs[0]

    res = client.search(Query("@country: " + ctry + " @city: %%" + city + "%% @postalcode: " + zip_postal).with_scores())
    if len(res.docs) > 0:
        res.docs[0].flag = 'C'
        return res.docs[0]

    res = client.search(Query("@country: " + ctry + " @state: " + state + " @postalcode: "
                              + zip_postal + " @postal_rank: 1").with_scores())
    if len(res.docs) > 0:
        res.docs[0].flag = 'E'
        return res.docs[0]

    res = client.search(Query("@country: " + ctry + " @city: %" + city + "% @state: " + state).with_scores())
    if len(res.docs) > 0:
        res.docs[0].flag = 'B'
        res.docs[0].postalcode = res.docs[0].primarypostal
        return res.docs[0]

    res = client.search(Query("@country: " + ctry + " @postalcode: " + zip_postal + " @postal_rank: 1").with_scores())
    if len(res.docs) > 0:
        res.docs[0].flag = 'G'
        return res.docs[0]

    res = client.search(Query("@country: " + ctry + " @state: " + state).with_scores())
    if len(res.docs) > 0:
        res.docs[0].flag = 'F'
        res.docs[0].actualcity = None
        res.docs[0].postalcode = None
        return res.docs[0]
    
    res = client.search(Query("@country: " + ctry + " @city: %" + city + "%").with_scores())
    if len(res.docs) > 0:
        res.docs[0].flag = 'D'
        res.docs[0].state = None
        res.docs[0].postalcode = None
        return res.docs[0]

def ot_search(city, state, zip_postal, ctry):
    print(city,' ', state, ' ', zip_postal)

    if city is None or city == '':
        city = 'XXXXX'
    else:
        city = re.sub(r'[^a-zA-Z0-9]','', city)
    if state is None or state == '':
        state = 'XX'

    if zip_postal is None or zip_postal == '' :
        zip_postal = 'XXXXX'
    elif ctry == 'USA' and len(str(zip_postal)) > 5:
        zip_postal = str(zip_postal)[0:5]
    else:
        zip_postal = re.sub(r'[^a-zA-Z0-9]', '', zip_postal)

    city = str(city)
    state = str(state)

    res = client.search(Query("@country: " + ctry + " @city: (%%" + city + "%%|" + city + "* ) @state: " + state
                              + " @postalcode:" + zip_postal).with_scores())
    if len(res.docs) > 0:
        res.docs[0].flag = 'A'
        return res.docs[0]


comment= '1: Query(\'SFO\').scorer(\'TFIDF.DOCNORM\').with_scores()'
q = Query('SFO').scorer('TFIDF.DOCNORM').with_scores()
res = client.search(q)

# the result has the total number of results, and a list of documents
printResult(comment,res,q)

comment= '2: Query(\'San Francisco\').scorer(\'TFIDF.DOCNORM\').with_scores()'
q = Query('San Francisco').scorer('TFIDF.DOCNORM').with_scores()
res = client.search(q)

# the result has the total number of results, and a list of documents
printResult(comment,res,q)

comment= '3: Query(\'San Francisco {USA}\').scorer(\'TFIDF.DOCNORM\').with_scores()'
q = Query('San Francisco {USA}').scorer('TFIDF.DOCNORM').with_scores()
res = client.search(q)

# the result has the total number of results, and a list of documents
printResult(comment,res,q)