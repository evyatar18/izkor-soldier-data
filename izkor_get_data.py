import codecs
import grequests    

def write_iterable(i, into):
    for elem in i:
        into.write(elem)
        into.write('\n')

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
    timer.print(current_indent + "time to do get requests: ")
    
    written = 0
    total = len(uuids)

    exception_uuids = set()
    
    for uuid, response in zip(uuids, responses):
        try:
            data = response.json()['data']
            death_date = to_string(from_string(data['death_date']))
        
            path = folder + "/" + death_date + "-" + uuid + ".json"

            # write BOM
            f = open(path, "wb")
            f.write(codecs.BOM_UTF8)
            f.close()

            # write the actual data
            f = open(path, "ab")
            f.write(json.dumps(data, ensure_ascii=False).encode("utf-8").decode().encode("utf-8"))
            f.close()

            written += 1
        except Exception as e:
            print(current_indent + "exception occurred on current batch.")
            print(current_indent + "uuid causing the exception: " + uuid)
            print(current_indent + str(e))
            exception_uuids.add(uuid)
    
    timer.stop()
    timer.print(current_indent + "total time for current batch: ")

    return written, exception_uuids

fmt = "https://izkorblobcdn.azureedge.net/fallenjson/{0}.json"

uuids_filename = "izkor.txt"
out_folder = "soldier_data"
exception_uuids = "exception_uuids.txt"

uuids = get_uuids(uuids_filename)
total = len(uuids)
written = 0
batch_size = 500

current_indent = ""
exception_uuids = set()

print("starting to get data of {0} soldiers. batch size: {1}".format(total, batch_size))
for batch in range(0, total, batch_size):
    current_indent = ""
    print(current_indent + "starting batch {0}.".format(batch))
    current_indent = "  "

    try:
        newly_written, exceptions += make_requests(uuids[batch:(batch+batch_size)], out_folder)
        written += newly_written
        exception_uuids = exception_uuids.union(exceptions)
        
        print(current_indent + "written data of {0}/{1}".format(written, total))
    except Exception as e:
        print(current_indent + "exception occurred on batch {0}".format(batch))
        print(current_indent + str(e))

    # newline separator
    print("")

ex = open(exception_uuids, "w")
write_iterable(exception_uuids, ex)
ex.close()

