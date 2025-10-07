import React from 'react';
import { Card } from '../ui/card';
import { DateRangeFilter } from '../filters/DateRangeFilter';
import { FilterBar } from '../filters/FilterBar';
import LiveTailTabCustom from '../custom/LiveTailTab';

export function DevComponentsView() {
  const [range] = React.useState({
    start: new Date(Date.now() - 3 * 24 * 60 * 60 * 1000),
    end: new Date(),
  });
  const [value, setValue] = React.useState(range);

  return (
    <div className="p-6 space-y-6">
      <h1>Components Playground</h1>
      <p className="text-muted-foreground">Visible only in development.</p>

      <LiveTailTabCustom defaultNamespace="default" defaultStream="demo" />

      <Card className="p-4 space-y-4">
        <div className="font-medium">Selectors</div>
        <div className="flex items-center gap-2">
          <LiveTailTabCustom defaultNamespace="default" defaultStream="demo" />
        </div>
      </Card>

      <Card className="p-4 space-y-4">
        <div className="font-medium">DateRangeFilter</div>
        <DateRangeFilter
          range={range}
          value={value}
          onChange={setValue}
          stepSeconds={60}
        />
        <div className="text-sm text-muted-foreground">
          Selected: {value.start.toISOString()} — {value.end.toISOString()}
        </div>
      </Card>

      <Card className="p-4 space-y-4">
        <div className="font-medium">FilterBar</div>
        <FilterBar
          initialStart={range.start}
          initialEnd={range.end}
          onApply={(r) => console.log('apply', r)}
          onReset={() => console.log('reset')}
        />
      </Card>
    </div>
  );
}

{/** 
  npx shadcn@latest rm https://www.shadcn.io/registry/code-block.json
      <div className="py-4 hover:bg-muted/90">
                      <Tabs defaultValue="json" className="w-full">
                        <div className="flex items-center justify-between mb-2">
                          <h4 className="font-medium text-xs text-muted-foreground">Payload</h4>
                          <TabsList className="h-7">
                            <TabsTrigger value="json" className="text-xs px-2">JSON</TabsTrigger>
                            <TabsTrigger value="text" className="text-xs px-2">Text</TabsTrigger>
                            <TabsTrigger value="xml" className="text-xs px-2">XML</TabsTrigger>
                          </TabsList>
                        </div>
                        <TabsContent value="json">
                          <CodeBlock data={[{ language: 'json', filename: 'payload.json', code: formatJsonPretty(selectedEvent.payload) }]} defaultValue="json" className="max-h-[50vh] w-full border-0 rounded-none">
                            <CodeBlockBody>
                              {({ language, code }) => (
                                <CodeBlockItem value={language} className="text-xs [&_code]:whitespace-pre-wrap [&_.line]:whitespace-pre-wrap [&_code]:break-words [&_.line]:break-words [&_code]:overflow-auto">
                                  <CodeBlockContent language={language as any} themes={{ light: 'vitesse-light', dark: 'vitesse-dark' }} className="whitespace-pre-wrap break-words">
                                    {code}
                                  </CodeBlockContent>
                                </CodeBlockItem>
                              )}
                            </CodeBlockBody>
                          </CodeBlock>
                        </TabsContent>
                        <TabsContent value="text">
                          <CodeBlock data={[{ language: 'text', filename: 'payload.txt', code: selectedEvent.payload }]} defaultValue="text" className="max-h-[50vh] w-full border-0 rounded-none">
                            <CodeBlockBody>
                              {({ language, code }) => (
                                <CodeBlockItem value={language} className="text-xs [&_code]:whitespace-pre-wrap [&_.line]:whitespace-pre-wrap [&_code]:break-words [&_.line]:break-words [&_code]:overflow-auto">
                                  <CodeBlockContent language={'text' as any} syntaxHighlighting={false} className="whitespace-pre-wrap break-words">
                                    {code}
                                  </CodeBlockContent>
                                </CodeBlockItem>
                              )}
                            </CodeBlockBody>
                          </CodeBlock>
                        </TabsContent>
                        <TabsContent value="xml">
                          <CodeBlock data={[{ language: 'xml', filename: 'payload.xml', code: formatXmlPretty(selectedEvent.payload) }]} defaultValue="xml" className="max-h-[50vh] w-full border-0 rounded-none">
                            <CodeBlockBody>
                              {({ language, code }) => (
                                <CodeBlockItem value={language} className="text-xs [&_code]:whitespace-pre-wrap [&_.line]:whitespace-pre-wrap [&_code]:break-words [&_.line]:break-words [&_code]:overflow-auto">
                                  <CodeBlockContent language={language as any} themes={{ light: 'vitesse-light', dark: 'vitesse-dark' }} className="whitespace-pre-wrap break-words">
                                    {code}
                                  </CodeBlockContent>
                                </CodeBlockItem>
                              )}
                            </CodeBlockBody>
                          </CodeBlock>
                        </TabsContent>
                      </Tabs>
                    </div>
                    */}

export default DevComponentsView;





/**
 * {/* No Data State 
                {!retryLoading && !dlqLoading && !statsLoading && !retryError && !dlqError && !statsError && 
                 retryMessages.length === 0 && dlqMessages.length === 0 && (
                    <div className="flex items-center justify-center py-8">
                        <div className="text-center">
                            <div className="text-sm text-muted-foreground mb-2">No retry or DLQ messages found</div>
                            <div className="text-xs text-muted-foreground">
                                Messages will appear here when they fail processing and are retried or moved to DLQ.
                            </div>
                            <div className="mt-4">
                                <button 
                                    onClick={() => {
                                        console.log('Testing API endpoints...');
                                        fetch('/v1/streams/retry?namespace=default&stream=demo')
                                            .then(res => res.json())
                                            .then(data => console.log('Retry API response:', data))
                                            .catch(err => console.error('Retry API error:', err));
                                    }}
                                    className="px-3 py-1 text-xs bg-blue-100 text-blue-800 rounded hover:bg-blue-200"
                                >
                                    Test API Connection
                                </button>
                            </div>
                        </div>
                    </div>
                )}
 */