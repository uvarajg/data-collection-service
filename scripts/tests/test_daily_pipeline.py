#!/usr/bin/env python3
"""
Test version of daily pipeline - runs with simulated data for testing
"""

import asyncio
import json
import os
from datetime import datetime, timedelta
from pathlib import Path

async def test_pipeline():
    """Test the daily pipeline with minimal operations"""
    print("🧪 TESTING DAILY PIPELINE")
    print("=" * 50)

    # Simulate each step
    steps = [
        "Input data refresh",
        "Input data report generation",
        "Data collection",
        "Data collection report",
        "Data validation",
        "Validation report",
        "Email reports"
    ]

    results = {}

    for i, step in enumerate(steps, 1):
        print(f"\n{i}. {step}...")

        # Simulate processing time
        await asyncio.sleep(1)

        # Simulate success/failure (all success for test)
        success = True
        duration = 1.0

        results[step] = {
            'status': 'success' if success else 'failed',
            'duration_seconds': duration
        }

        if success:
            print(f"   ✅ {step} completed ({duration}s)")
        else:
            print(f"   ❌ {step} failed ({duration}s)")

    # Summary
    print("\n" + "=" * 50)
    print("📊 TEST SUMMARY")
    print("=" * 50)

    successful = sum(1 for r in results.values() if r['status'] == 'success')
    total = len(results)

    print(f"Steps completed: {successful}/{total}")
    print(f"Success rate: {(successful/total)*100:.1f}%")

    # Save test results
    test_report = {
        'test_timestamp': datetime.now().isoformat(),
        'results': results,
        'summary': {
            'successful_steps': successful,
            'total_steps': total,
            'success_rate': (successful/total)*100
        }
    }

    report_file = f"test_pipeline_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(report_file, 'w') as f:
        json.dump(test_report, f, indent=2)

    print(f"📄 Test report saved: {report_file}")
    print("✅ Test completed successfully!")

if __name__ == "__main__":
    asyncio.run(test_pipeline())