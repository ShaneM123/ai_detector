use anyhow::{Ok, Result as AnyhowResult};
use hypertext::prelude::*;

pub fn homepage() -> AnyhowResult<String> {
    let response_html = rsx! {
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <div>
            <form action="/submit" method="post" class="form-example">
      <div class="form-example">
        <label for="email">"Enter email to check for ai:" </label>
        <input type="text" name="email" id="email" required />
      </div>
      <div class="form-example">
        <input type="submit" value="submit!" />
      </div>
    </form>
    </div>
        }
    .render()
    .into_inner();
    Ok(response_html)
}
