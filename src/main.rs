use todolist_rust;
use future_rust;

fn main() -> Result<(), std::io::Error> {
    future_rust::run();
    Ok(())

}

fn p(p: &[i32]) -> Vec<i32> {
    let len = p.len() as i32;

    (1..=len)
        .map(|t| {
            p.iter().position(|&x| x == t).map(|idx| idx as i32 + 1)
        })
        .map(|opt| {
            opt.and_then(|t| p.iter().position(|&x| x == t).map(|idx| idx as i32 + 1))
        })
        .collect::<Option<Vec<i32>>>()
        .unwrap_or_else(|| vec![]) 
}
