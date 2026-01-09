package db

import (
	"github.com/Filecoin-Titan/titan/api/types"
)

// UpdateNodeDynamicInfoFast uses a temporary table for fast batch updates
// This is optimized for large batches (1000+ nodes)
func (n *SQLDB) UpdateNodeDynamicInfoFast(infos []types.NodeDynamicInfo) error {
	if len(infos) == 0 {
		return nil
	}

	// For small batches, use the existing method
	if len(infos) < 500 {
		return n.UpdateNodeDynamicInfo(infos)
	}

	tx, err := n.db.Beginx()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	// Create temporary table
	_, err = tx.Exec(`
		CREATE TEMPORARY TABLE IF NOT EXISTS temp_node_updates (
			node_id VARCHAR(255) PRIMARY KEY,
			nat_type VARCHAR(50),
			last_seen DATETIME,
			online_duration INT,
			disk_usage FLOAT,
			bandwidth_up BIGINT,
			bandwidth_down BIGINT,
			titan_disk_usage FLOAT,
			available_disk_space FLOAT,
			download_traffic BIGINT,
			upload_traffic BIGINT
		)
	`)
	if err != nil {
		return err
	}

	// Clear temporary table if it exists from previous run
	_, err = tx.Exec(`TRUNCATE TABLE temp_node_updates`)
	if err != nil {
		// If TRUNCATE fails, try DELETE
		_, err = tx.Exec(`DELETE FROM temp_node_updates`)
		if err != nil {
			return err
		}
	}

	// Batch insert into temporary table
	stmt, err := tx.Preparex(`
		INSERT INTO temp_node_updates 
		(node_id, nat_type, last_seen, online_duration, disk_usage, 
		 bandwidth_up, bandwidth_down, titan_disk_usage, available_disk_space, 
		 download_traffic, upload_traffic)
		VALUES (?,?,?,?,?,?,?,?,?,?,?)
	`)
	if err != nil {
		return err
	}

	for _, info := range infos {
		_, err = stmt.Exec(
			info.NodeID, info.NATType, info.LastSeen,
			info.OnlineDuration, info.DiskUsage,
			info.BandwidthUp, info.BandwidthDown,
			info.TitanDiskUsage, info.AvailableDiskSpace,
			info.DownloadTraffic, info.UploadTraffic,
		)
		if err != nil {
			stmt.Close()
			return err
		}
	}
	stmt.Close()

	// Single UPDATE with JOIN
	_, err = tx.Exec(`
		UPDATE ` + nodeInfoTable + ` n
		INNER JOIN temp_node_updates t ON n.node_id = t.node_id
		SET 
			n.nat_type = t.nat_type,
			n.last_seen = t.last_seen,
			n.online_duration = n.online_duration + t.online_duration,
			n.disk_usage = t.disk_usage,
			n.bandwidth_up = t.bandwidth_up,
			n.bandwidth_down = t.bandwidth_down,
			n.titan_disk_usage = t.titan_disk_usage,
			n.available_disk_space = t.available_disk_space,
			n.download_traffic = t.download_traffic,
			n.upload_traffic = t.upload_traffic
	`)
	if err != nil {
		return err
	}

	// Drop temporary table
	_, err = tx.Exec(`DROP TEMPORARY TABLE IF EXISTS temp_node_updates`)
	if err != nil {
		log.Warnf("Failed to drop temporary table: %s", err.Error())
		// Don't fail the transaction for this
	}

	return tx.Commit()
}
