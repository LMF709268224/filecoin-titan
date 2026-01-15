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
	if len(infos) < 100 {
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
		) ENGINE=InnoDB
	`)
	if err != nil {
		return err
	}

	// Batch insert into temporary table using multi-valued INSERT
	const batchSize = 250
	values := make([]interface{}, 0, batchSize*11)

	for i := 0; i < len(infos); i += batchSize {
		end := i + batchSize
		if end > len(infos) {
			end = len(infos)
		}

		batch := infos[i:end]
		query := `INSERT INTO temp_node_updates 
			(node_id, nat_type, last_seen, online_duration, disk_usage, 
			 bandwidth_up, bandwidth_down, titan_disk_usage, available_disk_space, 
			 download_traffic, upload_traffic) VALUES `

		values = values[:0]
		for j, info := range batch {
			if j > 0 {
				query += ","
			}
			query += "(?,?,?,?,?,?,?,?,?,?,?)"
			values = append(values,
				info.NodeID, info.NATType, info.LastSeen,
				info.OnlineDuration, info.DiskUsage,
				info.BandwidthUp, info.BandwidthDown,
				info.TitanDiskUsage, info.AvailableDiskSpace,
				info.DownloadTraffic, info.UploadTraffic)
		}

		_, err = tx.Exec(query, values...)
		if err != nil {
			return err
		}
	}

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
	_, err = tx.Exec(`DROP TEMPORARY TABLE temp_node_updates`)
	if err != nil {
		log.Warnf("Failed to drop temporary table: %s", err.Error())
	}

	return tx.Commit()
}
