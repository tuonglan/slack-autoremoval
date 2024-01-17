import requests, math

class APIFailed(Exception): pass
class APIDownloadFailed(Exception): pass

def _get_messages(token, channel, start, end, limit=1000, user=None):
    url = "https://slack.com/api/conversations.history?channel=%s&oldest=%s&latest=%s&limit=%s" % (channel, start, end, limit)
    headers = {"Authorization": "Bearer %s" % token, "Content-Type": "application/x-www-form-urlencoded"}

    # Get the message
    re = requests.get(url, headers=headers)
    result = re.json()
    if not result['ok']:
        raise APIFailed(result['error'])

    msgs = [m for m in result['messages'] if user == None or m['user'] == user]
    return msgs, result['has_more']

def download_file(token, url, filename):
    try:
        r = requests.get(url, headers = {"Authorization": "Bearer %s" % token})
    except Exception as e:
        raise APIDownloadFailed("%s" % e)

    with open(filename, 'wb') as f:
        f.write(r.content)

def get_messages(token, channel, start, end, user=None):
    all_msgs = []
    errs = []
    err_count = 0

    # Allow max 11 error 
    while err_count < 11:
        try:
            msgs, has_more = _get_messages(token, channel, start, end, user=user)
        except APIFailed as e:
            err_count += 1
            errs.append(e)
            continue
            
        all_msgs.extend(msgs)
        if not has_more:
            break
        end = math.floor(float(msgs[-1]['ts']))

    return all_msgs, errs

def delete_message(token, channel, ts):
    url = "https://slack.com/api/chat.delete"
    headers = {"Authorization": "Bearer %s" % token, "Content-Type": "application/json;charset=utf-8"}
    data = {'channel': channel, 'ts': ts}

    re = requests.post(url, headers=headers, json=data)
    result = re.json()
    if not result['ok']:
        raise APIFailed(result['error'])

def get_replies(token, channel, ts):
    url = "https://slack.com/api/conversations.replies?channel=%s&ts=%s" % (channel, ts)
    headers = {"Authorization": "Bearer %s" % token, "Content-Type": "application/json;charset=utf-8"}
    
    re = requests.get(url, headers=headers)
    result = re.json()
    if not result['ok']:
        raise APIFailed(result['error'])

    return result['messages']
