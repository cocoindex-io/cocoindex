// BAML Schema for Slide Transcript Extraction

class SlideTranscript {
  speaker_notes string @description("Suggested speaker notes or narrative for the slide")
}

function ExtractSlideTranscript(slide_image: image) -> SlideTranscript {
  client Gemini
  prompt #"
    You are analyzing a presentation slide image to create natural speaker notes for text-to-speech.

    Analyze the slide and generate speaker notes that:
    - Go straight to the topic without any greetings, introductions, or filler phrases
    - Do NOT start with phrases like "Good morning", "Hello", "Welcome", "Today we'll discuss", etc.
    - Begin immediately with the actual content
    - Combine all textual content, bullet points, and visual elements into a natural narrative
    - Use a conversational speaking tone while remaining focused and concise
    - Present the information in a logical flow suitable for audio narration

    CRITICAL: You MUST respond with valid JSON only. Do not include any explanatory text before or after the JSON.

    {{ _.role("user") }}
    {{ slide_image }}

    {{ ctx.output_format }}
  "#
}

client<llm> Gemini {
  provider google-ai
  options {
    model gemini-2.5-flash
    api_key env.GEMINI_API_KEY
  }
}
