use anyhow::Result;
use console::style;
use serde::Serialize;
use std::fmt::Display;
use tabled::{Table, Tabled};

pub fn print_success(message: &str) {
    println!("{} {}", style("✓").green().bold(), message);
}

#[allow(dead_code)]
pub fn print_error(message: &str) {
    eprintln!("{} {}", style("✗").red().bold(), message);
}

pub fn print_warning(message: &str) {
    println!("{} {}", style("⚠").yellow().bold(), message);
}

pub fn print_info(message: &str) {
    println!("{} {}", style("ℹ").blue().bold(), message);
}

pub fn print_section(title: &str) {
    println!("\n{}", style(title).bold().underlined());
}

pub fn print_key_value(key: &str, value: impl Display) {
    println!("  {}: {}", style(key).cyan(), value);
}

pub fn print_json<T: Serialize>(data: &T) -> Result<()> {
    let json = serde_json::to_string_pretty(data)?;
    println!("{}", json);
    Ok(())
}

#[allow(dead_code)]
pub fn print_table<T: Tabled>(data: Vec<T>) {
    if data.is_empty() {
        print_info("No data to display");
        return;
    }

    let table = Table::new(data);
    println!("{}", table);
}

pub struct ProgressBar {
    inner: indicatif::ProgressBar,
}

#[allow(dead_code)]
impl ProgressBar {
    pub fn new(len: u64) -> Self {
        let pb = indicatif::ProgressBar::new(len);
        pb.set_style(
            indicatif::ProgressStyle::default_bar()
                .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})")
                .unwrap()
                .progress_chars("#>-"),
        );
        Self { inner: pb }
    }

    pub fn inc(&self, delta: u64) {
        self.inner.inc(delta);
    }

    pub fn finish_with_message(&self, msg: &str) {
        self.inner.finish_with_message(msg.to_string());
    }

    pub fn set_message(&self, msg: &str) {
        self.inner.set_message(msg.to_string());
    }
}

pub struct Spinner {
    inner: indicatif::ProgressBar,
}

impl Spinner {
    pub fn new(message: &str) -> Self {
        let sp = indicatif::ProgressBar::new_spinner();
        sp.set_style(
            indicatif::ProgressStyle::default_spinner()
                .template("{spinner:.green} {msg}")
                .unwrap(),
        );
        sp.set_message(message.to_string());
        sp.enable_steady_tick(std::time::Duration::from_millis(100));
        Self { inner: sp }
    }

    pub fn finish_with_message(&self, msg: &str) {
        self.inner.finish_with_message(msg.to_string());
    }

    #[allow(dead_code)]
    pub fn set_message(&self, msg: &str) {
        self.inner.set_message(msg.to_string());
    }
}

pub fn confirm(prompt: &str) -> Result<bool> {
    Ok(dialoguer::Confirm::new()
        .with_prompt(prompt)
        .interact()?)
}
