using System;
using System.Collections.Generic;
using System.Linq;

namespace QuickTechDataSyncService.Models
{
    public class SyncResult
    {
        public DateTime StartTime { get; set; } = DateTime.UtcNow;
        public DateTime EndTime { get; set; } = DateTime.UtcNow;
        public string DeviceId { get; set; } = string.Empty;
        public string EntityType { get; set; } = string.Empty;
        public bool Success { get; set; } = false;
        public string ErrorMessage { get; set; } = string.Empty;

        private Dictionary<string, int> _recordCounts = new Dictionary<string, int>();
        public Dictionary<string, int> RecordCounts
        {
            get => _recordCounts;
            set => _recordCounts = value ?? new Dictionary<string, int>();
        }

        public TimeSpan Duration => EndTime - StartTime;

        public int RecordCountSum => RecordCounts.Values.Sum();

        // Helper property to get a comma-separated string of record counts
        public string RecordCountSummary => string.Join(", ",
            RecordCounts.Select(kv => $"{kv.Key}: {kv.Value}"));

        // Helper method to get total modifications
        public int GetTotalModifications()
        {
            int total = 0;
            foreach (var kvp in RecordCounts)
            {
                if (kvp.Key.StartsWith("Deleted"))
                {
                    total += kvp.Value;
                }
                else if (!kvp.Key.Contains("Deleted"))
                {
                    total += kvp.Value;
                }
            }
            return total;
        }
    }
}