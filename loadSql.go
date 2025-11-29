package loadSql

import (
	"database/sql"
	_ "embed"
	"fmt"
	"log"
)

//go:embed job.sql
var jobSQL string

//go:embed worker.sql
var workerSQL string

//go:embed notify.sql
var notifySQL string

//go:embed master.sql
var masterSQL string

var JobFunctions = []string{
	"init_job",
	"insert_job",
	"update_job_initial",
	"update_job_final",
	"update_job_final_encrypted",
	"update_stale_jobs",
	"delete_job",
	"select_job",
	"select_all_jobs",
	"select_all_jobs_by_worker_rid",
	"select_all_jobs_by_search",
	"add_retention_archive",
	"remove_retention_archive",
	"delete_stale_jobs",
	"select_job_from_archive",
	"select_all_jobs_from_archive",
	"select_all_jobs_from_archive_by_search",
}
var WorkerFunctions = []string{
	"init_worker",
	"insert_worker",
	"update_worker",
	"delete_worker",
	"delete_stale_workers",
	"select_worker",
	"select_all_workers",
	"select_all_workers_by_search",
	"select_all_connections",
}
var MasterFunctions = []string{
	"init_master",
	"update_master",
	"select_master",
}
var NotifyFunctions = []string{
	"notify_event",
}

func LoadJobSql(db *sql.DB, force bool) error {
	if !force {
		exist, err := checkFunctions(db, JobFunctions)
		if err != nil {
			return fmt.Errorf("error checking existing job functions: %w", err)
		}
		if exist {
			return nil
		}
	}

	_, err := db.Exec(jobSQL)
	if err != nil {
		return fmt.Errorf("error executing job SQL: %w", err)
	}

	exist, err := checkFunctions(db, JobFunctions)
	if err != nil {
		return fmt.Errorf("error checking existing functions: %w", err)
	}
	if !exist {
		return fmt.Errorf("not all required SQL functions were created")
	}

	fmt.Println("SQL job functions loaded successfully")

	return nil
}

func LoadWorkerSql(db *sql.DB, force bool) error {
	if !force {
		exist, err := checkFunctions(db, WorkerFunctions)
		if err != nil {
			return fmt.Errorf("error checking existing worker functions: %w", err)
		}
		if exist {
			return nil
		}
	}

	_, err := db.Exec(workerSQL)
	if err != nil {
		return fmt.Errorf("error executing worker SQL: %w", err)
	}

	exist, err := checkFunctions(db, WorkerFunctions)
	if err != nil {
		return fmt.Errorf("error checking existing functions: %w", err)
	}
	if !exist {
		return fmt.Errorf("not all required SQL functions were created")
	}

	fmt.Println("SQL worker functions loaded successfully")

	return nil
}

func LoadNotifySql(db *sql.DB, force bool) error {
	if !force {
		exist, err := checkFunctions(db, NotifyFunctions)
		if err != nil {
			return fmt.Errorf("error checking existing notify functions: %w", err)
		}
		if exist {
			return nil
		}
	}

	_, err := db.Exec(notifySQL)
	if err != nil {
		return fmt.Errorf("error executing notify SQL: %w", err)
	}

	exist, err := checkFunctions(db, NotifyFunctions)
	if err != nil {
		return fmt.Errorf("error checking existing functions: %w", err)
	}
	if !exist {
		return fmt.Errorf("not all required SQL functions were created")
	}

	log.Println("SQL notify functions loaded successfully")

	return nil
}

func LoadMasterSql(db *sql.DB, force bool) error {
	if !force {
		exist, err := checkFunctions(db, MasterFunctions)
		if err != nil {
			return fmt.Errorf("error checking existing master functions: %w", err)
		}
		if exist {
			return nil
		}
	}

	_, err := db.Exec(masterSQL)
	if err != nil {
		return fmt.Errorf("error executing master SQL: %w", err)
	}

	exist, err := checkFunctions(db, MasterFunctions)
	if err != nil {
		return fmt.Errorf("error checking existing functions: %w", err)
	}
	if !exist {
		return fmt.Errorf("not all required SQL functions were created")
	}

	fmt.Println("SQL master functions loaded successfully")

	return nil
}

func checkFunctions(db *sql.DB, sqlFunctions []string) (bool, error) {
	var allExist bool
	for _, f := range sqlFunctions {
		err := db.QueryRow(
			`SELECT EXISTS(SELECT 1 FROM pg_proc WHERE proname = $1);`,
			f,
		).Scan(&allExist)
		if err != nil {
			return false, fmt.Errorf("error checking existence of function %s: %w", f, err)
		}

		if !allExist {
			log.Printf("Function %s does not exist", f)
			break
		}
	}
	return allExist, nil
}
