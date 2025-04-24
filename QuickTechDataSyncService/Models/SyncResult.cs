// QuickTechDataSyncService/Models/SyncResult.cs
using System;
using System.Collections.Generic;

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
        public Dictionary<string, int> RecordCounts { get; set; } = new Dictionary<string, int>();

        public TimeSpan Duration => EndTime - StartTime;
    }
}