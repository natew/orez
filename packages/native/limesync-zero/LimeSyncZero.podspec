require 'json'

package = JSON.parse(File.read(File.join(__dir__, '../package.json')))

Pod::Spec.new do |s|
  s.name = 'LimeSyncZero'
  s.version = package['version']
  s.summary = 'Lime Sync native Zero client'
  s.homepage = 'https://github.com/natew/orez'
  s.license = package['license']
  s.author = 'Lime Sync contributors'
  s.platform = :ios, min_ios_version_supported
  s.source = { :git => 'https://github.com/natew/orez.git' }
  s.source_files = 'cpp/**/*.{hpp,cpp,mm}'
  s.libraries = 'sqlite3'
  load File.join(__dir__, 'nitrogen/generated/ios/LimeSyncZero+autolinking.rb')
  add_nitrogen_files(s)
  s.dependency 'React-jsi'
  s.dependency 'React-callinvoker'
  install_modules_dependencies(s)
end
