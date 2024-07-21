import logging
import pandas as pd
from drain3 import TemplateMiner
from drain3.template_miner_config import TemplateMinerConfig

logger = logging.getLogger(__name__)

# Setup Drain3 log parser
config = TemplateMinerConfig()
template_miner = TemplateMiner(config=config)


def parse_logs(file_paths):
    logger.info(f"Parsing logs from {len(file_paths)} files")
    structured_logs = []
    for file_path in file_paths:
        try:
            with open(file_path, 'r') as log_file:
                for line in log_file:
                    cluster = template_miner.add_log_message(line)
                    structured_logs.append({
                        'cluster_id': cluster["cluster_id"],
                        'log_template': cluster["template_mined"],
                        'log_message': line.strip()
                    })
        except Exception as e:
            logger.error(f"Error parsing log file {file_path}: {str(e)}")

    logger.info(f"Parsed {len(structured_logs)} log entries in total")
    return pd.DataFrame(structured_logs)
