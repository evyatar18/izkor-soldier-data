import codecs
import grequests

fmt = "https://izkorblobcdn.azureedge.net/fallenjson/{0}.json"    

def get_uuids(f):
    f = open(f, "r")
    uuids = []
    
    while True:
        ln = f.readline().strip()
        if len(ln) == 0:
            break
        
        if not ln.startswith("en"):
            print("continued")
            print(ln)
            continue
        
        uuids.append(ln)

    f.close()
    return uuids

def from_string(st):
    from datetime import datetime
    
    return datetime.strptime(st, "%d-%m-%Y")

def to_string(dt):
    return dt.strftime("%Y-%m-%d")

def make_requests(uuids, folder):
    import json, timer
    
    timer = timer.timer()
    timer.start()
    requests = [grequests.get(fmt.format(uuid)) for uuid in uuids]
    responses = grequests.map(requests)
    timer.print("time to do get requests: ")
    
    written = 0
    total = len(uuids)
    
    for uuid, response in zip(uuids, responses):
        data = response.json()['data']
        death_date = to_string(from_string(data['death_date']))
        
        path = folder + "/" + death_date + "-" + uuid

        # write BOM
        f = open(path, "wb")
        f.write(codecs.BOM_UTF8)
        f.close()

        # write the actual data
        f = open(path, "ab")
        f.write(json.dumps(data, ensure_ascii=False).encode("utf-8").decode().encode("utf-8"))
        f.close()

        written += 1

    timer.stop()
    timer.print("total time for current batch: ")

    return written


uuids_filename = "izkor.txt"
out_folder = "soldier_data"

uuids = get_uuids(uuids_filename)
total = len(uuids)
written = 0
batch_size = 500

print("starting to get data of {0} soldiers. batch size: {1}".format(total, batch_size))
for batch in range(0, total, batch_size):
    print("starting batch {0}.".format(batch))

    try:
        written += make_requests(uuids[batch:(batch+batch_size)], out_folder)
        print("written data of {0}/{1}".format(written, total))
    except Exception as e:
        print("exception occurred on batch {0}".format(batch))
        print(e)
