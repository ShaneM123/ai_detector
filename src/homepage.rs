use anyhow::{Ok, Result as AnyhowResult};
use hypertext::prelude::*;

pub fn homepage() -> AnyhowResult<String> {
    let response_html = rsx! {
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <div>
            <form action="/submit" method="post" class="form-example">
      <div class="form-example">
        <label for="email">"Check if an Email is written by AI: " </label>
        <input type="text" name="email" id="email" placeholder="paste email here to check..." required />
      </div>
      <div class="form-example">
        <input type="submit" value="Submit!" />
      </div>
    </form>
    <div>
    <a href="https://github.com/ShaneM123/ai_detector">Source Code</a>
    </div>
    <div>
    <a href="https://www.linkedin.com/in/shanemoloney123/">Linkedin</a>
    </div>
    </div>
        }
    .render()
    .into_inner();
    Ok(response_html)
}
