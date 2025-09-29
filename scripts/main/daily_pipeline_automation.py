#!/usr/bin/env python3
"""
Daily Pipeline Automation Script

Executes the complete daily data pipeline:
1. Refresh input data (US stocks from GitHub)
2. Generate input data refresh report
3. Run data collection service for previous day
4. Generate data collection report
5. Run data validation service
6. Generate validation report
7. Email all reports

Designed to run at 4 AM daily to collect previous day's market data.
"""

import asyncio
import json
import os
import sys
import smtplib
import subprocess
from datetime import datetime, timedelta, date
from pathlib import Path
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import time
import glob

# Add data collection service path
sys.path.append('/workspaces/data-collection-service/src')

class DailyPipelineRunner:
    def __init__(self):
        self.start_time = datetime.now()
        self.reports = {}
        self.load_environment()

        # Calculate previous business day for data collection
        self.target_date = self.get_previous_business_day()

        print("=" * 80)
        print("🚀 DAILY PIPELINE AUTOMATION STARTED")
        print("=" * 80)
        print(f"📅 Run Date: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"📊 Target Data Date: {self.target_date}")
        print("=" * 80)

    def load_environment(self):
        """Load environment variables from .env file"""
        # Look for .env in the root directory of data-collection-service
        env_path = Path('/workspaces/data-collection-service/.env')
        if env_path.exists():
            with open(env_path, 'r') as f:
                for line in f:
                    if '=' in line and not line.startswith('#'):
                        key, value = line.strip().split('=', 1)
                        # Remove inline comments from the value
                        if '#' in value and key != 'GOOGLE_PRIVATE_KEY':  # Skip for private key which might contain #
                            value = value.split('#')[0].strip()
                        os.environ[key] = value

    def get_previous_business_day(self):
        """Get the previous business day (skip weekends)"""
        today = date.today()

        # If today is Monday (0), get Friday (subtract 3 days)
        # If today is Tuesday-Sunday (1-6), get previous day
        if today.weekday() == 0:  # Monday
            target = today - timedelta(days=3)
        elif today.weekday() == 6:  # Sunday
            target = today - timedelta(days=2)
        else:
            target = today - timedelta(days=1)

        return target.strftime('%Y-%m-%d')

    async def step1_refresh_input_data(self):
        """Step 1: Refresh US market stock data from GitHub"""
        print("\n" + "=" * 60)
        print("📊 STEP 1: REFRESHING INPUT DATA")
        print("=" * 60)

        try:
            # Change to data collection service directory
            original_cwd = os.getcwd()
            os.chdir('/workspaces/data-collection-service')

            print("🔄 Running: python scripts/main/collect_us_market_stocks.py")
            start_time = time.time()

            # Run the input data refresh
            result = subprocess.run([
                'python', 'scripts/main/collect_us_market_stocks.py'
            ], capture_output=True, text=True)  # No timeout - let it run to completion

            duration = time.time() - start_time

            if result.returncode == 0:
                print("✅ Input data refresh completed successfully")

                # Parse the output for statistics
                output_lines = result.stdout.split('\n')

                # Extract key metrics from output
                stats = {
                    'status': 'success',
                    'duration_seconds': round(duration, 2),
                    'raw_stocks_downloaded': 'Unknown',
                    'stocks_filtered': 'Unknown',
                    'stocks_enriched': 'Unknown',
                    'files_created': []
                }

                # Look for specific patterns in output
                for line in output_lines:
                    if 'Raw stocks downloaded:' in line:
                        stats['raw_stocks_downloaded'] = line.split(':')[1].strip()
                    elif 'Stocks > $2B:' in line:
                        stats['stocks_filtered'] = line.split(':')[1].strip()
                    elif 'Stocks enriched with YFinance:' in line:
                        stats['stocks_enriched'] = line.split(':')[1].strip()

                # Find created files
                data_dir = Path('/workspaces/data/input_source')
                today_str = datetime.now().strftime('%Y%m%d')

                for pattern in [f'raw_combined_{today_str}*.json',
                               f'enriched_yfinance_{today_str}*.json',
                               f'input_source_data_job_summary_{today_str}*.json']:
                    files = list(data_dir.glob(pattern))
                    if files:
                        stats['files_created'].extend([f.name for f in files])

                # Store latest enriched file for next step
                enriched_files = list(data_dir.glob(f'enriched_yfinance_{today_str}*.json'))
                if enriched_files:
                    self.latest_enriched_file = max(enriched_files, key=lambda x: x.stat().st_mtime)

                self.reports['input_data_refresh'] = stats
                print(f"📈 Raw stocks: {stats['raw_stocks_downloaded']}")
                print(f"📊 Filtered stocks: {stats['stocks_filtered']}")
                print(f"✨ Enriched stocks: {stats['stocks_enriched']}")
                print(f"⏱️  Duration: {stats['duration_seconds']}s")

            else:
                print(f"❌ Input data refresh failed with return code: {result.returncode}")
                print(f"Error output: {result.stderr}")

                self.reports['input_data_refresh'] = {
                    'status': 'failed',
                    'duration_seconds': round(duration, 2),
                    'error': result.stderr,
                    'return_code': result.returncode
                }

                return False

        except subprocess.TimeoutExpired:
            print("❌ Input data refresh timed out (10 minutes)")
            self.reports['input_data_refresh'] = {
                'status': 'timeout',
                'error': 'Process timed out after 10 minutes'
            }
            return False

        except Exception as e:
            print(f"❌ Input data refresh failed: {e}")
            self.reports['input_data_refresh'] = {
                'status': 'error',
                'error': str(e)
            }
            return False

        finally:
            os.chdir(original_cwd)

        return True

    def generate_input_data_report(self):
        """Step 2: Generate input data refresh report"""
        print("\n" + "=" * 60)
        print("📋 STEP 2: GENERATING INPUT DATA REPORT")
        print("=" * 60)

        report = self.reports.get('input_data_refresh', {})

        if report.get('status') == 'success':
            print("✅ Input data refresh was successful")
            print(f"📊 Summary: {report.get('stocks_enriched', 'Unknown')} stocks enriched")
            print(f"📁 Files created: {len(report.get('files_created', []))} files")
        else:
            print(f"❌ Input data refresh failed: {report.get('error', 'Unknown error')}")

        # Save detailed report
        report_file = f"/workspaces/data-collection-service/reports/input_data_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        os.makedirs(os.path.dirname(report_file), exist_ok=True)

        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2)

        print(f"📄 Report saved: {report_file}")
        return True

    async def step3_run_data_collection(self):
        """Step 3: Run data collection service for target date"""
        print("\n" + "=" * 60)
        print(f"🗂️  STEP 3: RUNNING DATA COLLECTION FOR {self.target_date}")
        print("=" * 60)

        try:
            print(f"📅 Collecting data for: {self.target_date}")
            start_time = time.time()

            # Change to data collection service directory
            original_cwd = os.getcwd()
            os.chdir('/workspaces/data-collection-service')

            # Run the automated collection script
            result = subprocess.run([
                'python', 'scripts/main/run_data_collection_with_dates.py', '--date', self.target_date, '--automated'
            ], capture_output=True, text=True)  # No timeout - let it run to completion

            duration = time.time() - start_time

            if result.returncode == 0:
                # Parse the JSON output from the script
                output_lines = result.stdout.split('\n')
                json_start = False
                json_lines = []

                for line in output_lines:
                    if 'COLLECTION RESULT:' in line:
                        json_start = True
                        continue
                    if json_start and line.strip() and not line.startswith('='):
                        json_lines.append(line)

                try:
                    # Parse the JSON result
                    json_str = '\n'.join(json_lines)
                    summary = json.loads(json_str)
                    summary['duration_seconds'] = round(duration, 2)

                    self.reports['data_collection'] = summary

                    print("✅ Data collection completed successfully")
                    print(f"📊 Total tickers: {summary.get('total_tickers', 0)}")
                    print(f"✅ Successful: {summary.get('successful_tickers', 0)}")
                    print(f"❌ Failed: {summary.get('failed_tickers', 0)}")
                    print(f"📈 Success rate: {summary.get('success_rate', 0)}%")
                    print(f"⏱️  Duration: {summary['duration_seconds']}s")

                except json.JSONDecodeError:
                    print("⚠️  Collection completed but couldn't parse results")
                    self.reports['data_collection'] = {
                        'status': 'completed_no_parse',
                        'target_date': self.target_date,
                        'duration_seconds': round(duration, 2),
                        'raw_output': result.stdout
                    }

            else:
                print(f"❌ Data collection failed with return code: {result.returncode}")
                print(f"Error: {result.stderr}")

                self.reports['data_collection'] = {
                    'status': 'failed',
                    'target_date': self.target_date,
                    'duration_seconds': round(duration, 2),
                    'error': result.stderr,
                    'return_code': result.returncode
                }
                return False

        except subprocess.TimeoutExpired:
            print("❌ Data collection timed out (60 minutes)")
            self.reports['data_collection'] = {
                'status': 'timeout',
                'target_date': self.target_date,
                'error': 'Collection timed out after 60 minutes'
            }
            return False

        except Exception as e:
            print(f"❌ Data collection failed: {e}")
            self.reports['data_collection'] = {
                'status': 'error',
                'target_date': self.target_date,
                'error': str(e)
            }
            return False

        finally:
            os.chdir(original_cwd)

        return True

    def generate_data_collection_report(self):
        """Step 4: Generate data collection report"""
        print("\n" + "=" * 60)
        print("📋 STEP 4: GENERATING DATA COLLECTION REPORT")
        print("=" * 60)

        report = self.reports.get('data_collection', {})

        if report.get('status') == 'success':
            print("✅ Data collection was successful")
            print(f"📈 Success rate: {report.get('success_rate', 0)}%")
            print(f"📊 Records collected: {report.get('successful_tickers', 0)}")
        else:
            print(f"❌ Data collection failed: {report.get('error', 'Unknown error')}")

        # Save detailed report
        report_file = f"/workspaces/data-collection-service/reports/data_collection_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        os.makedirs(os.path.dirname(report_file), exist_ok=True)

        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2)

        print(f"📄 Report saved: {report_file}")
        return True

    def step5_run_data_validation(self):
        """Step 5: Run data validation service"""
        print("\n" + "=" * 60)
        print("🔍 STEP 5: RUNNING DATA VALIDATION")
        print("=" * 60)

        try:
            # Change to validation service directory
            original_cwd = os.getcwd()
            os.chdir('/workspaces/data-validation-service')

            print(f"🔎 Validating data for: {self.target_date}")
            start_time = time.time()

            # Create a simple validation script for the target date
            validation_script = f"""#!/usr/bin/env python3
import json
import sys
from datetime import datetime
from pathlib import Path
from collections import defaultdict

def validate_data_for_date(target_date):
    data_path = Path("/workspaces/data/historical/daily")

    records = []
    ticker_count = 0
    error_tickers = []

    # Scan all ticker directories
    for ticker_dir in sorted(data_path.iterdir()):
        if not ticker_dir.is_dir():
            continue

        ticker = ticker_dir.name
        year, month, day = target_date.split('-')
        file_path = ticker_dir / year / month / f"{{target_date}}.json"

        if file_path.exists():
            try:
                with open(file_path, 'r') as f:
                    data = json.load(f)
                    records.append(data)
                    ticker_count += 1
            except Exception as e:
                error_tickers.append({{
                    'ticker': ticker,
                    'error': str(e)
                }})

    # Basic validation metrics
    validation_results = {{
        'target_date': target_date,
        'total_files_found': ticker_count,
        'total_records': len(records),
        'error_count': len(error_tickers),
        'success_rate': round((ticker_count / (ticker_count + len(error_tickers))) * 100, 2) if (ticker_count + len(error_tickers)) > 0 else 0,
        'validation_timestamp': datetime.now().isoformat(),
        'error_tickers': error_tickers[:10]  # Limit to first 10 errors
    }}

    # Data quality checks
    if records:
        complete_records = sum(1 for r in records if r.get('close') and r.get('volume'))
        validation_results['data_completeness'] = round((complete_records / len(records)) * 100, 2)

        # Check for technical indicators
        with_technical = sum(1 for r in records if r.get('technical', {{}}).get('sma_200'))
        validation_results['technical_indicators_coverage'] = round((with_technical / len(records)) * 100, 2)

        # Check for fundamental data
        with_fundamentals = sum(1 for r in records if r.get('fundamental', {{}}).get('market_cap'))
        validation_results['fundamental_data_coverage'] = round((with_fundamentals / len(records)) * 100, 2)

    return validation_results

if __name__ == "__main__":
    results = validate_data_for_date("{self.target_date}")

    # Save results
    output_file = f"validation_report_{{results['target_date'].replace('-', '_')}}_{{datetime.now().strftime('%Y%m%d_%H%M%S')}}.json"
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2)

    print(json.dumps(results, indent=2))
"""

            # Write and execute validation script
            script_path = f'temp_validation_{self.target_date.replace("-", "_")}.py'
            with open(script_path, 'w') as f:
                f.write(validation_script)

            # Run validation
            result = subprocess.run([
                'python', script_path
            ], capture_output=True, text=True)  # No timeout - let it run to completion

            duration = time.time() - start_time

            # Clean up temp script
            if os.path.exists(script_path):
                os.remove(script_path)

            if result.returncode == 0:
                print("✅ Data validation completed successfully")

                # Parse validation results
                try:
                    validation_results = json.loads(result.stdout)
                    validation_results['duration_seconds'] = round(duration, 2)
                    validation_results['status'] = 'success'

                    self.reports['data_validation'] = validation_results

                    print(f"📊 Files validated: {validation_results.get('total_files_found', 0)}")
                    print(f"📈 Success rate: {validation_results.get('success_rate', 0)}%")
                    print(f"📋 Data completeness: {validation_results.get('data_completeness', 0)}%")
                    print(f"🔧 Technical indicators: {validation_results.get('technical_indicators_coverage', 0)}%")
                    print(f"📊 Fundamental data: {validation_results.get('fundamental_data_coverage', 0)}%")

                except json.JSONDecodeError:
                    print("⚠️  Validation completed but couldn't parse results")
                    self.reports['data_validation'] = {
                        'status': 'success_no_parse',
                        'duration_seconds': round(duration, 2),
                        'raw_output': result.stdout
                    }

            else:
                print(f"❌ Data validation failed with return code: {result.returncode}")
                print(f"Error: {result.stderr}")

                self.reports['data_validation'] = {
                    'status': 'failed',
                    'duration_seconds': round(duration, 2),
                    'error': result.stderr,
                    'return_code': result.returncode
                }
                return False

        except subprocess.TimeoutExpired:
            print("❌ Data validation timed out (5 minutes)")
            self.reports['data_validation'] = {
                'status': 'timeout',
                'error': 'Validation timed out after 5 minutes'
            }
            return False

        except Exception as e:
            print(f"❌ Data validation failed: {e}")
            self.reports['data_validation'] = {
                'status': 'error',
                'error': str(e)
            }
            return False

        finally:
            os.chdir(original_cwd)

        return True

    def generate_validation_report(self):
        """Step 6: Generate validation report"""
        print("\n" + "=" * 60)
        print("📋 STEP 6: GENERATING VALIDATION REPORT")
        print("=" * 60)

        report = self.reports.get('data_validation', {})

        if report.get('status') == 'success':
            print("✅ Data validation was successful")
            print(f"📊 Quality score: {report.get('success_rate', 0)}%")
        else:
            print(f"❌ Data validation failed: {report.get('error', 'Unknown error')}")

        # Save detailed report
        report_file = f"/workspaces/data-collection-service/reports/validation_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        os.makedirs(os.path.dirname(report_file), exist_ok=True)

        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2)

        print(f"📄 Report saved: {report_file}")
        return True

    def step7_email_reports(self):
        """Step 7: Email all reports"""
        print("\n" + "=" * 60)
        print("📧 STEP 7: EMAILING REPORTS")
        print("=" * 60)

        try:
            # Prepare email content
            email_subject = f"Daily Data Pipeline Report - {self.target_date}"

            # Create comprehensive summary
            total_duration = (datetime.now() - self.start_time).total_seconds()

            email_body = f"""
Daily Data Pipeline Execution Report
====================================

Execution Date: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}
Target Data Date: {self.target_date}
Total Duration: {total_duration:.1f} seconds

PIPELINE SUMMARY:
================

1. INPUT DATA REFRESH:
   Status: {self.reports.get('input_data_refresh', {}).get('status', 'Not run')}
   Duration: {self.reports.get('input_data_refresh', {}).get('duration_seconds', 0)}s
   Stocks Enriched: {self.reports.get('input_data_refresh', {}).get('stocks_enriched', 'Unknown')}

2. DATA COLLECTION:
   Status: {self.reports.get('data_collection', {}).get('status', 'Not run')}
   Duration: {self.reports.get('data_collection', {}).get('duration_seconds', 0)}s
   Success Rate: {self.reports.get('data_collection', {}).get('success_rate', 0)}%
   Successful Tickers: {self.reports.get('data_collection', {}).get('successful_tickers', 0)}

3. DATA VALIDATION:
   Status: {self.reports.get('data_validation', {}).get('status', 'Not run')}
   Duration: {self.reports.get('data_validation', {}).get('duration_seconds', 0)}s
   Files Validated: {self.reports.get('data_validation', {}).get('total_files_found', 0)}
   Data Completeness: {self.reports.get('data_validation', {}).get('data_completeness', 0)}%

DETAILED REPORTS:
================
"""

            # Add detailed information for each step
            for step_name, report in self.reports.items():
                email_body += f"\\n{step_name.upper()}:\\n"
                email_body += json.dumps(report, indent=2)
                email_body += "\\n" + "-" * 50 + "\\n"

            # Setup email
            msg = MIMEMultipart()
            msg['From'] = os.getenv('EMAIL_FROM', 'noreply@algoalchemist.com')
            msg['To'] = os.getenv('EMAIL_TO', 'admin@algoalchemist.com')
            msg['Subject'] = email_subject

            # Attach body
            msg.attach(MIMEText(email_body, 'plain'))

            # Attach report files if they exist
            reports_dir = Path('/workspaces/data-collection-service/reports')
            if reports_dir.exists():
                today_str = datetime.now().strftime('%Y%m%d')
                for report_file in reports_dir.glob(f'*{today_str}*.json'):
                    try:
                        with open(report_file, 'rb') as f:
                            part = MIMEBase('application', 'octet-stream')
                            part.set_payload(f.read())
                            encoders.encode_base64(part)
                            part.add_header(
                                'Content-Disposition',
                                f'attachment; filename= {report_file.name}'
                            )
                            msg.attach(part)
                    except Exception as e:
                        print(f"⚠️  Could not attach {report_file.name}: {e}")

            # Send email
            email_user = os.getenv('EMAIL_USER')
            email_password = os.getenv('EMAIL_PASSWORD')

            if email_user and email_password:
                server = smtplib.SMTP('smtp.gmail.com', 587)
                server.starttls()
                server.login(email_user, email_password)
                text = msg.as_string()
                server.sendmail(msg['From'], msg['To'], text)
                server.quit()

                print("✅ Reports emailed successfully")
                print(f"📧 Sent to: {msg['To']}")

            else:
                print("⚠️  Email credentials not configured - saving report locally instead")

                # Save email content locally
                email_file = f"/workspaces/data-collection-service/reports/email_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
                with open(email_file, 'w') as f:
                    f.write(f"To: {msg['To']}\\n")
                    f.write(f"Subject: {msg['Subject']}\\n\\n")
                    f.write(email_body)

                print(f"📄 Email content saved: {email_file}")

            return True

        except Exception as e:
            print(f"❌ Failed to send email: {e}")
            print("📄 Reports are available in /workspaces/data-collection-service/reports/")
            return False

    async def run_pipeline(self):
        """Run the complete pipeline"""
        success_count = 0
        total_steps = 7

        # Step 1: Refresh input data
        if await self.step1_refresh_input_data():
            success_count += 1
        else:
            print("🛑 Stopping pipeline due to input data refresh failure")
            return False

        # Step 2: Generate input data report
        if self.generate_input_data_report():
            success_count += 1

        # Step 3: Run data collection
        if await self.step3_run_data_collection():
            success_count += 1
        else:
            print("⚠️  Data collection failed, but continuing with validation...")

        # Step 4: Generate data collection report
        if self.generate_data_collection_report():
            success_count += 1

        # Step 5: Run data validation
        if self.step5_run_data_validation():
            success_count += 1

        # Step 6: Generate validation report
        if self.generate_validation_report():
            success_count += 1

        # Step 7: Email reports
        if self.step7_email_reports():
            success_count += 1

        # Final summary
        print("\n" + "=" * 80)
        print("🏁 DAILY PIPELINE EXECUTION COMPLETE")
        print("=" * 80)

        total_duration = (datetime.now() - self.start_time).total_seconds()
        success_rate = (success_count / total_steps) * 100

        print(f"📊 Steps completed: {success_count}/{total_steps} ({success_rate:.1f}%)")
        print(f"⏱️  Total execution time: {total_duration:.1f} seconds")
        print(f"📅 Target data date: {self.target_date}")

        if success_count == total_steps:
            print("✅ All steps completed successfully!")
        elif success_count >= 5:
            print("⚠️  Pipeline completed with minor issues")
        else:
            print("❌ Pipeline completed with significant issues")

        print(f"📁 Reports available in: /workspaces/data-collection-service/reports/")
        print("=" * 80)

        return success_count >= 5  # Consider success if at least 5/7 steps passed

async def main():
    """Main entry point"""
    try:
        runner = DailyPipelineRunner()
        success = await runner.run_pipeline()

        # Exit with appropriate code
        sys.exit(0 if success else 1)

    except KeyboardInterrupt:
        print("\\n⚠️  Pipeline interrupted by user")
        sys.exit(130)
    except Exception as e:
        print(f"\\n❌ Pipeline failed with unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())