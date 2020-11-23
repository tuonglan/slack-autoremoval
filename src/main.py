import logging

import logger as Logger
from slack_bot import SlackBot
from slack_message_archiver import SlackMessageArchiver

LOG_LEVELS = {
    'debug':    logging.DEBUG,
    'info':     logging.INFO,
    'warning':  logging.WARNING,
    'error':    logging.ERROR,
    'critical': logging.CRITICAL
}

g_logger = None
g_slack = None


def archive_messages(args):
    g_logger.info("Start archiving message from %s to %s...", args.start_date, args.end_date)
    archiver = SlackMessageArchiver(args.token, args.user_token, args.channel, g_logger, conc=args.delete_conc)
    result = archiver.archive_messages(args.start_date, args.end_date, args.archive_path, download=not args.no_download, 
                                       delete=args.delete)
    g_logger.info("Number of lines archived: %d", result['msg_count'])
    g_logger.info("Number of files saved: %d", result['file_count'])
    g_logger.info("Number of general errors: %d", len(result['errs']))
    g_logger.info("Number of downloading errors: %d", len(result['download_errs']))
    g_logger.info("Number of deleting errors: %d", len(result['delete_errs']))

    if g_slack:
        msg = "Number of lines archived: %d\nNumber of files saved: %d" % (result['msg_count'], result['file_count'])
        if result['errs'] or result['download_errs'] or result['delete_errs']:
            msgs = "%s\nNumber of general erros: %d\nNumber of downloading erros %d\nNumber of deleting errors %d" % (
                        msg, len(result['errs']), len(result['download_errs']), len(result['delete_errs']))
            g_slack.post_alert("XXL Archive from %s to %s" % (args.start_date, args.end_date))
        else:
            g_slack.post_info("XXL Archive from %s to %s" % (args.start_date, args.end_date), msg)

if __name__ == '__main__':
    from argparse import ArgumentParser
    parser = ArgumentParser()
    sub_parsers = parser.add_subparsers(dest='subparser_name')
    parser.add_argument('--channel', type=str, help="Channel ID")
    parser.add_argument('--user', type=str, default=None, help='User ID of the messages')
    parser.add_argument('--token', type=str, help='Authentication Token for Bot user')
    parser.add_argument('--user_token', type=str, action='append', help='Specify user:token for deletion')
    parser.add_argument('--delete', action='store_true', help='Delete the message or not')
    parser.add_argument('--delete_conc', type=int, default=11, help='Concurrent deleting threads')
    parser.add_argument('--no_download', action='store_true', help='Prevent downloading files')
    parser.add_argument('--archive_path', type=str, help='Location for saving the messages')
    
    parser.add_argument('--slack_token', type=str, default='', help='Slack Bot token for notification')
    parser.add_argument('--slack_channels', type=str, help='Slack Bot channels for info, alert (seperated by commas)')
    parser.add_argument('--slack_notifee', type=str, help='ID of person for notification of alerts')

    parser.add_argument('--log_path', type=str, default='/tmp/slack-autoremoval', help='Location of logs')
    parser.add_argument('--log_level', type=str, default='error', choices=['critical', 'error', 'warning', 'info', 'debug'],
                        help='Level of console logs')
    parser.add_argument('--log_saving_period', type=int, default=11, help='Number of days for saving logs')

    parser_archive = sub_parsers.add_parser('archive')
    parser_archive.add_argument('--start_date', type=str, help='Starting date of the archive: YYYYMMDD')
    parser_archive.add_argument('--end_date', type=str, help='Ending date of the archive: YYYYMMDD')
    parser_archive.set_defaults(func=archive_messages)
    
    # Parser the pargument
    args = parser.parse_args()

    # Init the logger
    logger_prefix = args.subparser_name if args.subparser_name else 'main'
    g_logger = Logger.init_logger(logger_prefix, args.log_path, logging.DEBUG, LOG_LEVELS[args.log_level], args.log_saving_period)

    # Init the slack bot
    if args.slack_token:
        chs = args.slack_channels.split(',')
        ch_info = chs[0]
        ch_alert = chs[1] if len(chs) > 1 else None
        g_slack = SlackBot(args.slack_token, ch_info, args.slack_notifee, channel_alert=ch_alert)

    # Call the function
    args.func(args)

