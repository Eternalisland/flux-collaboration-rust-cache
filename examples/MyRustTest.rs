

fn main(){
    // 测试
    println!("Hello, world!");
    let str = String::from("Hello World!");

    let word = first_word(&str);

    println!("first word is: {}", word);
}

fn first_word(s: &str) -> &str {
    let bytes = s.as_bytes();

    for (i, &item) in bytes.iter().enumerate() {
        if item == b' ' {
            return &s[0..i];
        }
    }

    &s[..]
}