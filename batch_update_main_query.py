import firebase_admin
from firebase_admin import  firestore
from datetime import datetime
import os

collection_name = os.environ.get('collection_name','user_programs')
batch_size = os.environ.get('batch_size',200000)

firebase_admin.initialize_app()

def update_collection(request):
    
    db = firestore.client()    
    coll = db.collection(collection_name)
    current_ts_string = datetime.utcnow().strftime("%m/%d/%Y")
    current_ts_date = datetime.strptime(current_ts_string,"%m/%d/%Y").date()
    coll_ref = db.collection(collection_name).where(u'status',u'not-in',[u'CLAIMED',u'EXPIRED'])
    
    while True:
        docs = coll_ref.limit(int(batch_size)).stream()
        batch_counter = 1
        batch = db.batch()
        updated = 0
        for doc in docs:
            expiry = datetime.strptime(doc.to_dict()['expiry'], "%m/%d/%Y").date()
            if expiry<current_ts_date:
                updated=updated+1
                if batch_counter < 450:
                    batch.update(coll.document(doc.id), {u'status': 'EXPIRED'})     
                    batch_counter+=1
                else:
                    batch.update(coll.document(doc.id), {u'status': 'EXPIRED'})
                    batch.commit()
                    batch_counter = 1
                    batch = db.batch()
        batch.commit()
        if updated==0:
            break