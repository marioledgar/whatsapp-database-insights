import argparse
import os
import sys
from wa_analyzer.src.parser import WhatsappParser
from wa_analyzer.src.analyzer import WhatsappAnalyzer
from wa_analyzer.src.visualizer import ReportGenerator

def main():
    parser = argparse.ArgumentParser(description="WhatsApp Data Analyzer")
    parser.add_argument("--msgstore", default="msgstore.db", help="Path to msgstore.db")
    parser.add_argument("--wa", default="wa.db", help="Path to wa.db")
    parser.add_argument("--vcf", default="contacts.vcf", help="Path to contacts.vcf")
    parser.add_argument("--output", default="wa_report_output", help="Output directory")
    
    args = parser.parse_args()
    
    # Resolve absolute paths
    base_dir = os.getcwd()
    msgstore_path = os.path.abspath(args.msgstore)
    wa_path = os.path.abspath(args.wa)
    vcf_path = os.path.abspath(args.vcf)
    output_dir = os.path.abspath(args.output)
    
    print(f"Starting Analysis...")
    print(f"Msgstore: {msgstore_path}")
    print(f"WA DB: {wa_path}")
    print(f"VCF: {vcf_path}")
    print(f"Output: {output_dir}")
    
    # 1. Parse
    print("\n[1/4] Parsing Data...")
    parser_obj = WhatsappParser(msgstore_path, wa_path, vcf_path)
    df = parser_obj.get_merged_data()
    
    if df.empty:
        print("No data parsed! Exiting.")
        sys.exit(1)
        
    print(f"Parsed {len(df)} messages.")
    
    # 2. Analyze
    print("\n[2/4] Analyzing Data...")
    analyzer = WhatsappAnalyzer(df)
    basic_stats = analyzer.get_basic_stats()
    top_talkers = analyzer.get_top_talkers(15)
    hourly_activity = analyzer.get_hourly_activity()
    monthly_activity = analyzer.get_monthly_activity()
    avg_reply = analyzer.calculate_response_times()
    basic_stats['avg_reply_minutes'] = avg_reply
    gender_counts = analyzer.analyze_by_gender()
    
    print("Gathering text for wordcloud (this might take a moment)...")
    full_text = analyzer.get_wordcloud_text()
    
    # 3. Visualize
    print("\n[3/4] Generating Visualizations...")
    viz = ReportGenerator(output_dir)
    viz.plot_top_talkers(top_talkers)
    viz.plot_hourly_activity(hourly_activity)
    viz.plot_timeline(monthly_activity)
    viz.plot_gender_stats(gender_counts)
    viz.generate_wordcloud(full_text)
    
    # 4. Report
    print("\n[4/4] Writing Report...")
    viz.generate_report_md(basic_stats, basic_stats) # stats passed twice as simple_stats for now
    
    print(f"\nDone! Report generated in {output_dir}")

if __name__ == "__main__":
    main()
