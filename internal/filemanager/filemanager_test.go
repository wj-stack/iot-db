package filemanager

import (
	"iot-db/internal/config"
	"reflect"
	"sync"
	"testing"
)

func TestFileManager_AddFile(t *testing.T) {
	type fields struct {
		Config *config.Config
		mutex  sync.RWMutex
		files  map[string]struct{}
		opened map[string]*File
	}
	type args struct {
		name string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fm := &FileManager{
				Config: tt.fields.Config,
				Mutex:  tt.fields.mutex,
				files:  tt.fields.files,
				opened: tt.fields.opened,
			}
			if err := fm.AddFile(tt.args.name); (err != nil) != tt.wantErr {
				t.Errorf("AddFile() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestFileManager_CloseFile(t *testing.T) {
	type fields struct {
		Config *config.Config
		mutex  sync.RWMutex
		files  map[string]struct{}
		opened map[string]*File
	}
	type args struct {
		name string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fm := &FileManager{
				Config: tt.fields.Config,
				Mutex:  tt.fields.mutex,
				files:  tt.fields.files,
				opened: tt.fields.opened,
			}
			if err := fm.CloseFile(tt.args.name); (err != nil) != tt.wantErr {
				t.Errorf("CloseFile() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestFileManager_CreateTempFile(t *testing.T) {
	type fields struct {
		Config *config.Config
		mutex  sync.RWMutex
		files  map[string]struct{}
		opened map[string]*File
	}
	type args struct {
		name string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *File
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fm := &FileManager{
				Config: tt.fields.Config,
				Mutex:  tt.fields.mutex,
				files:  tt.fields.files,
				opened: tt.fields.opened,
			}
			got, err := fm.CreateTempFile(tt.args.name)
			if (err != nil) != tt.wantErr {
				t.Errorf("CreateTempFile() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("CreateTempFile() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestFileManager_DelFile(t *testing.T) {
	type fields struct {
		Config *config.Config
		mutex  sync.RWMutex
		files  map[string]struct{}
		opened map[string]*File
	}
	type args struct {
		name string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fm := &FileManager{
				Config: tt.fields.Config,
				Mutex:  tt.fields.mutex,
				files:  tt.fields.files,
				opened: tt.fields.opened,
			}
			if err := fm.DelFile(tt.args.name); (err != nil) != tt.wantErr {
				t.Errorf("DelFile() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestFileManager_GetCompactFileList(t *testing.T) {
	type fields struct {
		Config *config.Config
		mutex  sync.RWMutex
		files  map[string]struct{}
		opened map[string]*File
	}
	type args struct {
		shardSize     int
		nextShardSize int
		maxId         int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   map[int][]string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fm := &FileManager{
				Config: tt.fields.Config,
				Mutex:  tt.fields.mutex,
				files:  tt.fields.files,
				opened: tt.fields.opened,
			}
			if got := fm.GetCompactFileList(tt.args.shardSize, tt.args.nextShardSize, tt.args.maxId); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetCompactFileList() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestFileManager_InitDir(t *testing.T) {
	type fields struct {
		Config *config.Config
		mutex  sync.RWMutex
		files  map[string]struct{}
		opened map[string]*File
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fm := &FileManager{
				Config: tt.fields.Config,
				Mutex:  tt.fields.mutex,
				files:  tt.fields.files,
				opened: tt.fields.opened,
			}
			if err := fm.InitDir(); (err != nil) != tt.wantErr {
				t.Errorf("InitDir() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestFileManager_OpenFile(t *testing.T) {
	type fields struct {
		Config *config.Config
		mutex  sync.RWMutex
		files  map[string]struct{}
		opened map[string]*File
	}
	type args struct {
		name string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *File
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fm := &FileManager{
				Config: tt.fields.Config,
				Mutex:  tt.fields.mutex,
				files:  tt.fields.files,
				opened: tt.fields.opened,
			}
			got, err := fm.OpenFile(tt.args.name)
			if (err != nil) != tt.wantErr {
				t.Errorf("OpenFile() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("OpenFile() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestFileManager_ReTempName(t *testing.T) {
	type fields struct {
		Config *config.Config
		mutex  sync.RWMutex
		files  map[string]struct{}
		opened map[string]*File
	}
	type args struct {
		shardGroup   int
		shardGroupId int
		src          string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fm := &FileManager{
				Config: tt.fields.Config,
				Mutex:  tt.fields.mutex,
				files:  tt.fields.files,
				opened: tt.fields.opened,
			}
			if err := fm.ReTempName(tt.args.shardGroup, tt.args.shardGroupId, tt.args.src); (err != nil) != tt.wantErr {
				t.Errorf("ReTempName() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestFileManager_Remove(t *testing.T) {
	type fields struct {
		Config *config.Config
		mutex  sync.RWMutex
		files  map[string]struct{}
		opened map[string]*File
	}
	type args struct {
		name string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fm := &FileManager{
				Config: tt.fields.Config,
				Mutex:  tt.fields.mutex,
				files:  tt.fields.files,
				opened: tt.fields.opened,
			}
			if err := fm.Remove(tt.args.name); (err != nil) != tt.wantErr {
				t.Errorf("Remove() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestFileManager_SaveTempFile(t *testing.T) {
	type fields struct {
		Config *config.Config
		mutex  sync.RWMutex
		files  map[string]struct{}
		opened map[string]*File
	}
	type args struct {
		f *File
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fm := &FileManager{
				Config: tt.fields.Config,
				Mutex:  tt.fields.mutex,
				files:  tt.fields.files,
				opened: tt.fields.opened,
			}
			if err := fm.SaveTempFile(tt.args.f); (err != nil) != tt.wantErr {
				t.Errorf("SaveTempFile() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestFileManager_rename(t *testing.T) {
	type fields struct {
		Config *config.Config
		mutex  sync.RWMutex
		files  map[string]struct{}
		opened map[string]*File
	}
	type args struct {
		name string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fm := &FileManager{
				Config: tt.fields.Config,
				Mutex:  tt.fields.mutex,
				files:  tt.fields.files,
				opened: tt.fields.opened,
			}
			if err := fm.rename(tt.args.name); (err != nil) != tt.wantErr {
				t.Errorf("rename() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestNewFileManager(t *testing.T) {
	type args struct {
		Config *config.Config
	}
	tests := []struct {
		name string
		args args
		want *FileManager
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewFileManager(tt.args.Config); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewFileManager() = %v, want %v", got, tt.want)
			}
		})
	}
}
