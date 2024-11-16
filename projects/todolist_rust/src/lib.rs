/***
 * Project: To-Do List Manager (Day 1)
 * Description: A Rust-based command-line To-Do List Manager that allows users to:
 * 1. Add tasks to a list.
 * 2. View all tasks with their completion status.
 * 3. Mark tasks as completed.
 * 4. Save and load tasks from a JSON file.
 * 
 * Core Components:
 * - Task Struct:
 *   - Fields: 
 *     - `description` (String): The description of the task.
 *     - `completed` (bool): The completion status of the task.
 *     - `priority` (Option<i64>): The priority of the task (1 = Low, 2 = Medium, 3 = High).
 *     - `due_date` (Option<i64>): The due date of the task, represented in `YYYYMMDD` format.
 *   - Purpose: Represents a single task with its description, completion state, priority, and due date.
 * 
 * - TaskManager Struct:
 *   - Manages a collection of tasks and provides various methods.
 *   - Methods:
 *     - `add_task`: Adds a new task with a given description, priority, and due date.
 *     - `view_tasks`: Displays all tasks with their descriptions and completion status.
 *     - `mark_complete`: Marks a specified task as completed based on its index.
 *     - `to_json`: Saves the current list of tasks to a JSON file.
 *     - `from_json`: Loads tasks from a JSON file into the TaskManager.
 ***/

 use std::{fs::{self, File}, io::{BufWriter, Write as _}};
 use serde::{Deserialize, Serialize};
 use clap::Parser;
 
 /// Command-line argument parsing structure
 #[derive(Parser, Debug)]
 #[command(version, about, long_about = None)]
 struct Args {
    /// Add a new task with a specified description, priority (1-3), and due date (YYYYMMDD)
    /// 
    /// The format is: "task description,priority,duedate
    /// 
    /// - priority: 1 (low), 2 (medium), 3 (high)
    /// 
    /// - duedate: in the format YYYYMMDD
    /// 
    /// Example: "task description,1,20240909"
    #[clap(short, long, default_value = "",verbatim_doc_comment)]
    add: String,
 
     /// List all tasks with their completion status.
     #[arg(long, default_value_t = false)]
     list: bool,
 
     /// Mark a specific task as complete by index (1-based).
     #[arg(short, long)]
     complete: Option<i64>,
 
     /// Save tasks to a JSON file.
     #[arg(long, default_value_t = true)]
     save: bool,
 
     /// Load tasks from a specified JSON file path.
     #[arg(long)]
     load: Option<String>,
 }
 
 /// Struct for representing individual tasks.
 #[derive(Serialize, Debug, Deserialize)]
 pub struct Task {
     pub description: String,
     pub completed: bool,
     pub priority: Option<i64>,
     pub due_date: Option<i64>,
 }
 
 /// Struct for managing a collection of tasks and providing various methods.
 #[derive(Serialize, Debug, Deserialize)]
 pub struct TaskManager {
     tasks: Vec<Task>,
 }
 
 impl TaskManager {
     /// Creates a new TaskManager instance with an empty task list.
     pub fn new() -> Self {
         Self { tasks: Vec::new() }
     }
 
     /// Adds a new task to the TaskManager.
     pub fn add_task(&mut self, task: Task) {
         self.tasks.push(task);
     }
 
     /// Prints the current list of tasks along with their completion status.
     pub fn view_tasks(&self) {
         println!("Total tasks: {}", self.tasks.len());
         for (index, task) in self.tasks.iter().enumerate() {
             println!(
                 "{}: {} - {} - Priority: {} - Due Date: {}",
                 index + 1,
                 task.description,
                 if task.completed { "Completed" } else { "Not completed" },
                 task.priority.unwrap_or(0), // Default to 0 if no priority is set
                 task.due_date.unwrap_or(0)   // Default to 0 if no due date is set
             );
         }
     }
 
     /// Marks a task as complete by its index.
     pub fn mark_complete(&mut self, index: usize) -> Result<(), std::io::Error> {
         match self.tasks.get_mut(index) {
             Some(task) => {
                 task.completed = true;
                 println!("Task number {} marked as completed", index + 1);
             },
             None => {
                 println!("Error: Task index {} is out of range.", index + 1);
                 return Err(std::io::Error::new(std::io::ErrorKind::NotFound, "Task not found"));
             },
         };
         self.to_json() // Save the updated task list after marking the task as complete
     }
 
     /// Saves the current list of tasks to a JSON file.
     pub fn to_json(&mut self) -> Result<(), std::io::Error> {
         let file = File::create("task_manager.json")?;
         let mut writer = BufWriter::new(file);
         serde_json::to_writer(&mut writer, &self)?;
         writer.flush()?;
         Ok(())
     }
 
     /// Loads tasks from a JSON file and updates the current TaskManager.
     pub fn from_json(&mut self, path: &str) -> Result<(), std::io::Error> {
         let file = File::open(path)?;
         let json: TaskManager = serde_json::from_reader(file)?;
         self.tasks = json.tasks;
         Ok(())
     }
 
     /// Initializes the task manager by checking for an existing JSON file.
     pub fn initial_load(&mut self) -> Result<(), std::io::Error> {
         if !fs::exists("task_manager.json")? {
             let file = File::create("task_manager.json")?;
             serde_json::to_writer(&file, &self)?;
         }
         self.from_json("task_manager.json")?;
         Ok(())
     }
 }
 
 /// Entry point of the program
 pub fn app() -> Result<(), std::io::Error> {
     let args = Args::parse(); // Parse command-line arguments
     let mut task_manager = TaskManager::new();
     task_manager.initial_load()?; // Load initial tasks
 
     // Add new task if the description is provided
     if !args.add.is_empty() {
         let parts: Vec<&str> = args.add.split(',').collect();
         if parts.len() != 3 {
             println!("Invalid task format. Expected format: description,priority,duedate");
             return Ok(());
         }
 
         let priority: Option<i64> = parts[1].parse().ok();
         if priority.is_none() || priority.unwrap() < 1 || priority.unwrap() > 3 {
             println!("Invalid priority. Expected 1 (low), 2 (medium), or 3 (high).");
             return Ok(());
         }
 
         let due_date: Option<i64> = parts[2].parse().ok();
         if due_date.is_none() {
             println!("Invalid due date. Expected a valid date in the format YYYYMMDD.");
             return Ok(());
         }
 
         task_manager.add_task(Task {
             description: parts[0].to_string(),
             completed: false,
             priority,
             due_date,
         });
 
         task_manager.to_json()?; // Save to JSON file after adding
     }
 
     // List all tasks if requested
     if args.list {
         task_manager.view_tasks();
     }
 
     // Mark a task as complete if an index is provided
     if let Some(index) = args.complete {
         if index > 0 {
             let task_index = (index - 1) as usize;
             task_manager.mark_complete(task_index)?;
         }
     }
 
     // Save tasks if requested
     if args.save {
         task_manager.to_json()?;
     }
 
     // Load tasks from a specified path if requested
     if let Some(path) = args.load {
         task_manager.from_json(&path)?;
     }
 
     Ok(())
 }
 
 #[cfg(test)]
 mod tests {
     use super::*;
 
     #[test]
     fn test_task_creation() {
         let mut task_manager = TaskManager::new();
         task_manager.add_task(Task {
             description: String::from("Sample task"),
             completed: false,
             priority: Some(2),
             due_date: Some(20241111),
         });
 
         assert_eq!(task_manager.tasks.get(0).unwrap().description, "Sample task");
         assert_eq!(task_manager.tasks.get(0).unwrap().completed, false);
         assert_eq!(task_manager.tasks.get(0).unwrap().priority, Some(2));
         assert_eq!(task_manager.tasks.get(0).unwrap().due_date, Some(20241111));
     }
 
     #[test]
     fn test_task_completed_mark() -> Result<(), std::io::Error> {
         let mut task_manager = TaskManager::new();
         task_manager.add_task(Task {
             description: String::from("Complete me"),
             completed: false,
             priority: Some(3),
             due_date: Some(20241212),
         });
 
         task_manager.mark_complete(0)?;
 
         assert_eq!(task_manager.tasks.get(0).unwrap().completed, true);
         Ok(())
     }
 }
 