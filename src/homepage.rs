use anyhow::{Ok, Result as AnyhowResult};
use hypertext::prelude::*;

pub fn homepage() -> AnyhowResult<String> {
    let response_html = rsx! {
        <div>
            <form action="/submit" method="post" class="form-example">
      <div class="form-example">
        <label for="name">"Enter email to check for ai:" </label>
        <input type="text" name="name" id="name" required />
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
