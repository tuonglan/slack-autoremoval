import requests, json

class SlackBot:
    def __init__(self, token, channel, notifee, channel_warning=None, channel_alert=None):
        self._token = token
        self._channel = channel
        self._channel_warning = channel_warning
        self._channel_alert = channel_alert
        self._notifee = notifee

    def post_message(self, title, text, color='#7CD197', channel=None):
        headers = {
            'Authorization': "Bearer %s" % self._token,
            'Content-type': 'application/json'
            }
        payload = {
            'channel': channel or self._channel,
            'text': title,
            'attachments': [{'text': text, 'color': color}]
            }

        rs = requests.post('https://slack.com/api/chat.postMessage', headers=headers, data=json.dumps(payload))
        data = rs.json()
        if not data['ok']:
            raise Exception(data['error'])
        else:
            return rs.text

    def post_info(self, title, text):
        return self.post_message(title, text)

    def post_warning(self, title, text):
        return self.post_message(title, text, '#EBB424', channel=self._channel_warning)

    def post_alert(self, title, text):
        new_title = "%s <@%s>" % (title, self._notifee) if self._notifee else title
        return self.post_message(new_title, text, '#D40E0D', self._channel_alert or self._channel_warning)

