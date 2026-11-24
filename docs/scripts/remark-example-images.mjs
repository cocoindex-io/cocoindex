// Example article images sit below the shared Scene/Run it header. Preserve the
// authored Markdown (also served to agents), and improve only its HTML rendering.
import { visit } from 'unist-util-visit';
import metadata from '../src/data/example-image-metadata.json' with { type: 'json' };

const ASSET_BASE = 'https://cocoindex.io/blobs/docs-v1/img/examples/';

export default function remarkExampleImages() {
  return (tree, file) => {
    if (!/[\\/]example-posts[\\/]/.test(file.path ?? '')) return;

    // These covers were retired in favor of the Scene header. Omitting the node
    // avoids downloading an image that the old CSS merely hid.
    const firstParagraph = tree.children.findIndex((node) => node.type === 'paragraph');
    const first = tree.children[firstParagraph];
    if (first?.children.length === 1 && first.children[0].type === 'image' &&
        first.children[0].url.startsWith(ASSET_BASE) && /\/cover\.png$/.test(first.children[0].url)) {
      tree.children.splice(firstParagraph, 1);
    }

    visit(tree, 'image', (node) => {
      const size = metadata[node.url];
      if (!size && node.url.startsWith(ASSET_BASE)) {
        throw new Error(`Missing example image dimensions: ${node.url}. Run scripts/update-example-image-metadata.mjs with the blobs/public checkout.`);
      }
      node.data ??= {};
      node.data.hProperties = {
        ...node.data.hProperties,
        ...(size ?? {}),
        loading: 'lazy',
        decoding: 'async',
      };
    });

    visit(tree, 'paragraph', (node) => {
      const isDiagram = (image) => image.type === 'image' &&
        image.url.startsWith(ASSET_BASE) && /\.svg$/.test(image.url);
      if (!node.children.some(isDiagram)) return;
      // Standalone instructional diagrams remain readable at the original size,
      // including without JavaScript. A real link also works with keyboard/AT.
      if (node.children.every((child) => child.type === 'image' ||
          (child.type === 'text' && !child.value.trim()))) {
        node.data = { ...node.data, hName: 'figure', hProperties: { className: ['example-diagram'] } };
      }
      node.children = node.children.flatMap((image) => !isDiagram(image) ? [image] : [image, {
        type: 'link', url: image.url,
        data: { hProperties: {
          className: ['example-diagram-open'], target: '_blank', rel: ['noopener'],
          ariaLabel: `Open diagram at full size: ${image.alt || 'Pipeline diagram'} (opens in a new tab)`,
        } },
        children: [{ type: 'text', value: 'Open diagram at full size' }],
      }]);
    });
  };
}
