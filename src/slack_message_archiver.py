import os
import json, time, gzip
from datetime import datetime, timezone
import concurrent.futures as Futures

import slack_api
    
def _str_to_ts(s, fmt):
    return int(datetime.strptime(s, fmt).replace(tzinfo=timezone.utc).timestamp())
def _ts_to_str(ts, fmt):
    return datetime.utcfromtimestamp(ts).strftime(fmt)

class SlackMessageArchiver:
    def __init__(self, token, user_tokens, channel, logger, conc=11):
        self._logger = logger
        self._channel = channel
        self._token = token
        self._conc=conc

        # Init user tokens
        self._user_tokens = {}
        if user_tokens:
            for ut in user_tokens:
                data = ut.split(':')
                self._user_tokens[data[0]] = data[1]
        print("====== %s" % self._user_tokens)

    # Working on daily basic
    def _download_files(self, files_path, files):
        basepath = os.path.basename(files_path)
        download_errs = []

        for f in files:
            if not 'url_private_download' in f:
                print("Invalid file: %s" % f)
                continue

            strs = f['url_private_download'].split('/')
            basename = "%s-%s" % (strs[-3], strs[-1])
            filename = os.path.join(files_path, basename)
            rel_filename = os.path.join(basepath, basename)

            # Download
            try:
                slack_api.download_file(self._token, f['url_private_download'], filename)
            except slack_api.APIDownloadFailed as e:
                self._logger.error("Can't download file %s to %s: %s" % (f['url_private_download'], filename, e))
                download_errs.append(f['url_private_download'])
                continue
        
            # Save file information
            f['ZZZ_localfile'] = rel_filename
        return download_errs

    def _delete_messages(self, msgs):
        delete_errs = []
        start_ts = time.time()
        
        # Define delete functions
        def delete_msg(msg):
            if not msg['user'] in self._user_tokens:
                self._logger.error("\tCan't delete msg \"%s\", channel %s, user %s, ts %s: Unauthorized", msg['text'],
                                    self._channel, msg['user'], msg['ts'])
                return ('Unauthorized', msg)
            else:
                while True:
                    try:
                        slack_api.delete_message(self._user_tokens[msg['user']], self._channel, msg['ts'])
                    except slack_api.APIFailed as e:
                        self._logger.error("\tCan't delete msg \"%s\", channel %s, user %s, ts %s: %s", msg['text'],
                                           self._channel, msg['user'], msg['ts'], e)
                        if str(e) == 'ratelimited':
                            time.sleep(1.5)
                            continue
                        return (e, msg)
                    except Exception as e:
                        # Only continue when thread=1
                        if self._conc == 1:
                            self._logger.error("\tCan't delete msg \"%s\", channel %s, user %s, ts %s: %s", msg['text'],
                                               self._channel, msg['user'], msg['ts'], e)
                            time.sleep(1.5)
                            continue
                        else:  
                            raise e

                    break

            return None

        # Init delete threads & run
        with Futures.ThreadPoolExecutor(max_workers=self._conc) as executor:
            tasks = {executor.submit(delete_msg, msg): msg for msg in msgs}
            for future in Futures.as_completed(tasks):
                msg = tasks[future]
                result = future.result()
                if result:
                    delete_errs.append(result)

        self._logger.info("Messages deleted: %d, time: %.3f seconds", len(msgs) - len(delete_errs), time.time()-start_ts)
        return delete_errs


    def archive_messages(self, start_date, end_date, archive_path, download=True, delete=False):
        start_ts = _str_to_ts(start_date, "%Y%m%d")
        end_ts = _str_to_ts(end_date, "%Y%m%d")
        
        total_errs = []
        total_download_errs = []
        total_delete_errs = []
        msg_count = 0
        file_count = 0

        while start_ts < end_ts:
            # Get the monthly directory
            year_month = _ts_to_str(start_ts, "%Y%m")
            archive_path_month = os.path.join(archive_path, year_month)
            files_path = os.path.join(archive_path_month, "files")
            if not os.path.isdir(files_path):
                os.makedirs(files_path)

            # Get the messages
            msg_date = _ts_to_str(start_ts, "%Y%m%d")
            self._logger.info("Archiving channel %s on date %s to location %s", self._channel, msg_date, archive_path_month)
            msgs, errs = slack_api.get_messages(self._token, self._channel, start_ts, start_ts+24*3600)
            if errs:
                total_errs.extend(errs)
                for err in errs:
                    self._logger.critical(err)
                self._logger.error("%d errors when downloading the messages", len(errs))
            if not msgs:
                self._logger.info("Messages on date %s is empty, continue the next day", msg_date)
                start_ts += 24*3600
                continue
            msg_count += len(msgs)

            # Save the files
            if download:
                self._logger.info("Downloaded %d message(s), saving files if any", len(msgs))
                files = [f for msg in msgs if msg.get('files', False) for f in msg['files']]
                file_count += len(files)
                if files:
                    download_errs = self._download_files(files_path, files)
                    if download_errs:
                        total_download_errs.extend(download_errs)
                        self._logger.error("Can't download %d files:", len(download_errs))
                    self._logger.info("Downloaded %d file(s) into %s", len(files) - len(download_errs), files_path)

            # Save the message to archive file
            filename = os.path.join(archive_path_month, "%s.gz" % msg_date)
            with gzip.open(filename,  'w') as writer:
                writer.write(json.dumps(msgs, indent=4).encode('utf-8'))
            self._logger.info("Messages on date %s saved to %s", msg_date, filename)

            start_ts += 24*3600

            # Delete the message if invokea
            if delete:
                self._logger.info("Start deleting %d messages on date %s with %d thread(s)", len(msgs), msg_date, self._conc)
                delete_errs = self._delete_messages(msgs)
                if delete_errs:
                    total_delete_errs.extend(delete_errs)
                    self._logger.error("Can't delete %d messages", len(delete_errs))

        return {'errs': total_errs, 'download_errs': total_download_errs, 'delete_errs': total_delete_errs,
                'msg_count': msg_count, 'file_count': file_count}


