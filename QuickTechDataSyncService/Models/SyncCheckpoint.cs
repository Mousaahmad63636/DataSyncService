using System;

namespace QuickTechDataSyncService.Models
{
    public class SyncCheckpoint
    {
        public int Id { get; set; }
        public string DeviceId { get; set; } = string.Empty;
        public string EntityType { get; set; } = string.Empty;
        public DateTime? LastSyncTime { get; set; }
        public int LastRecordId { get; set; }
        public DateTime? LastDeleteCheck { get; set; }
        public string? CheckpointData { get; set; }
        public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
        public DateTime UpdatedAt { get; set; } = DateTime.UtcNow;
    }
}