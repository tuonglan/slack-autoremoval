import logging

import logger as Logger
from slack_message_archiver import SlackMessageArchiver

LOG_LEVELS = {
    'debug':    logging.DEBUG,
    'info':     logging.INFO,
    'warning':  logging.WARNING,
    'error':    logging.ERROR,
    'critical': logging.CRITICAL
}

g_logger = None


def archive_messages(args):
    g_logger.info("Start archiving message from %s to %s...", args.start_date, args.end_date)
    archiver = SlackMessageArchiver(args.token, args.user_token, args.channel, g_logger, conc=args.delete_conc)
    archiver.archive_messages(args.start_date, args.end_date, args.archive_path, download=not args.no_download, delete=args.delete)

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

    # Call the function
    args.func(args)

